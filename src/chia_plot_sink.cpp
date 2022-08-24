/*
 * chia_plot_sink.cpp
 *
 *  Created on: Aug 24, 2022
 *      Author: mad
 */

#include <string>
#include <cstring>
#include <vector>
#include <stdexcept>
#include <iostream>
#include <mutex>
#include <thread>
#include <map>
#include <chrono>
#include <csignal>
#include <cmath>
#include <random>
#include <algorithm>

#include <experimental/filesystem>

#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

#include <cxxopts.hpp>


static int g_port = 1337;
static int g_server = -1;
static bool g_do_run = true;
static bool g_force_shutdown = false;

static std::mutex g_mutex;
static std::map<uint64_t, std::shared_ptr<std::thread>> g_threads;


inline
int64_t get_time_millis() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

inline
::sockaddr_in get_sockaddr_byname(const std::string& endpoint, int port)
{
	::sockaddr_in addr;
	::memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	::hostent* host = ::gethostbyname(endpoint.c_str());
	if(!host) {
		throw std::runtime_error("could not resolve: '" + endpoint + "'");
	}
	::memcpy(&addr.sin_addr.s_addr, host->h_addr_list[0], host->h_length);
	return addr;
}

inline
void recv_bytes(void* dst, const int fd, const size_t num_bytes)
{
	auto num_left = num_bytes;
	auto* dst_= (uint8_t*)dst;
	while(num_left > 0) {
		const auto num_read = ::recv(fd, dst_, num_left, 0);
		if(num_read < 0) {
			throw std::runtime_error("recv() failed with: " + std::string(strerror(errno)));
		}
		num_left -= num_read;
		dst_ += num_read;
	}
}

inline
void send_bytes(const int fd, const void* src, const size_t num_bytes)
{
	if(::send(fd, src, num_bytes, 0) != num_bytes) {
		throw std::runtime_error("send() failed with: " + std::string(strerror(errno)));
	}
}

static
void trigger_shutdown(int sig)
{
	if(g_force_shutdown) {
		::exit(-4);
	}
	std::cout << std::endl;

	g_do_run = false;
	g_force_shutdown = true;

	const int sock = ::socket(AF_INET, SOCK_STREAM, 0);
	::sockaddr_in addr = get_sockaddr_byname("localhost", g_port);
	::connect(sock, (::sockaddr*)&addr, sizeof(addr));
	::close(sock);
}

static
void copy_func(const uint64_t job, const int fd, const size_t num_bytes, const std::string& dst_path)
{
	auto* file = fopen(dst_path.c_str(), "wb");
	if(file) {
		std::lock_guard<std::mutex> lock(g_mutex);
		std::cout << "Started copy to " << dst_path << " (" << num_bytes / pow(1024, 3) << " GiB)" << std::endl;
	} else {
		std::lock_guard<std::mutex> lock(g_mutex);
		std::cerr << "fopen('" << dst_path << "') failed with: " << strerror(errno) << std::endl;
	}
	const auto time_begin = get_time_millis();

	char buffer[256 * 1024] = {};
	size_t num_left = num_bytes;

	while(file && num_left)
	{
		const auto num_read = ::recv(fd, buffer, std::min(num_left, sizeof(buffer)), 0);
		if(num_read < 0) {
			std::lock_guard<std::mutex> lock(g_mutex);
			std::cerr << "recv() failed with: " << strerror(errno) << std::endl;
			break;
		}
		num_left -= num_read;

		if(fwrite(buffer, 1, num_read, file) != num_read)
		{
			std::lock_guard<std::mutex> lock(g_mutex);
			std::cerr << "fwrite('" << dst_path << "') failed with: " << strerror(errno) << std::endl;
			break;
		}
	}
	::close(fd);

	while(file && fclose(file)) {
		{
			std::lock_guard<std::mutex> lock(g_mutex);
			std::cerr << "fclose('" << dst_path << "') failed with: " << strerror(errno) << std::endl;
		}
		std::this_thread::sleep_for(std::chrono::seconds(60));
	}
	if(num_left) {
		std::remove(dst_path.c_str());
		std::lock_guard<std::mutex> lock(g_mutex);
		std::cerr << "Deleted " << dst_path << std::endl;
	}
	{
		std::lock_guard<std::mutex> lock(g_mutex);
		if(auto thread = g_threads[job]) {
			thread->detach();
		}
		g_threads.erase(job);

		if(!num_left) {
			const auto elapsed = (get_time_millis() - time_begin) / 1e3;
			std::cout << "Finished copy to " << dst_path << ", took " << elapsed << " sec, "
					<< num_bytes / pow(1024, 2) / elapsed << " MB/s" << std::endl;
		}
	}
}


int main(int argc, char** argv)
{
	std::signal(SIGINT, trigger_shutdown);
	std::signal(SIGTERM, trigger_shutdown);

	cxxopts::Options options("cuda_plot_sink",
		"Final copy engine to receive plots from one or more plotters via TCP and distribute to multiple disks in parallel.\n\n"
		"Usage: cuda_plot_sink -- /mnt/disk0/ /mnt/disk1/ ...\n"
	);

	std::vector<std::string> dir_list;

	options.allow_unrecognised_options().add_options()(
		"p, port", "Port to listen on (default = 1337)", cxxopts::value<int>(g_port))(
		"d, destination", "List of destination folders", cxxopts::value<std::vector<std::string>>(dir_list))(
		"help", "Print help");

	options.parse_positional("destination");

	const auto args = options.parse(argc, argv);

	if(args.count("help") || dir_list.empty()) {
		std::cout << options.help({""}) << std::endl;
		return 0;
	}
	for(const auto& dir : dir_list) {
		std::cout << "Final Directory: " << dir << " (" << int(std::experimental::filesystem::space(dir).free / pow(1024, 3)) << " GiB free)" << std::endl;
	}

	// create server socket
	g_server = ::socket(AF_INET, SOCK_STREAM, 0);
	if(g_server < 0) {
		throw std::runtime_error("socket() failed with: " + std::string(strerror(errno)));
	}
	{
		int enable = 1;
		if(::setsockopt(g_server, SOL_SOCKET, SO_REUSEADDR, (char*)&enable, sizeof(int)) < 0) {
			std::cerr << "setsockopt(SO_REUSEADDR) failed with: " << strerror(errno);
		}
	}
	{
		::sockaddr_in addr = get_sockaddr_byname("0.0.0.0", g_port);
		if(::bind(g_server, (::sockaddr*)&addr, sizeof(addr)) < 0) {
			throw std::runtime_error("bind() failed with: " + std::string(strerror(errno)));
		}
	}
	if(::listen(g_server, 10) < 0) {
		throw std::runtime_error("listen() failed with: " + std::string(strerror(errno)));
	}
	std::cout << "Listening on port " << g_port << std::endl;

	uint64_t job_counter = 0;
	std::default_random_engine rand_engine;

	while(g_do_run)
	{
		::sockaddr_in addr = {};
		::socklen_t addr_len = 0;
		const int fd = ::accept(g_server, (::sockaddr*)&addr, &addr_len);
		if(!g_do_run) {
			::close(fd);
			break;
		}
		if(fd >= 0) {
			try {
				uint64_t file_size = 0;
				recv_bytes(&file_size, fd, 8);

				auto dirs = dir_list;
				std::shuffle(dirs.begin(), dirs.end(), rand_engine);

				const std::string* out = nullptr;
				for(const auto& dir : dirs) {
					if(std::experimental::filesystem::space(dir).free > file_size + 4096) {
						out = &dir;
						break;
					}
				}
				if(!out) {
					const char cmd = 0;
					send_bytes(fd, &cmd, 1);
					throw std::runtime_error("no space left for " + std::to_string(file_size) + " bytes");
				}
				{
					const char cmd = 1;
					send_bytes(fd, &cmd, 1);
				}
				uint16_t file_name_len = 0;
				recv_bytes(&file_name_len, fd, 2);

				std::vector<char> file_name(file_name_len);
				recv_bytes(file_name.data(), fd, file_name_len);

				const auto file_path = *out + '/' + std::string(file_name.data(), file_name.size());
				{
					std::lock_guard<std::mutex> lock(g_mutex);
					g_threads[job_counter] = std::make_shared<std::thread>(&copy_func, job_counter, fd, file_size, file_path);
				}
				job_counter++;
			}
			catch(const std::exception& ex) {
				::close(fd);
				std::lock_guard<std::mutex> lock(g_mutex);
				std::cerr << "accept() failed with: " << ex.what() << std::endl;
			}
		} else {
			break;
		}
	}
	::close(g_server);

	{
		std::lock_guard<std::mutex> lock(g_mutex);
		std::cout << "Waiting for jobs to finish ..." << std::endl;
	}
	decltype(g_threads) threads;
	{
		std::lock_guard<std::mutex> lock(g_mutex);
		threads = g_threads;
		g_threads.clear();
	}
	for(const auto& entry : threads) {
		entry.second->join();
	}
	return 0;
}

