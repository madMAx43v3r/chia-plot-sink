/*
 * chia_plot_copy.cpp
 *
 *  Created on: Jan 14, 2023
 *      Author: mad
 */

#include <mutex>
#include <string>
#include <chrono>
#include <cmath>

#define _SILENCE_EXPERIMENTAL_FILESYSTEM_DEPRECATION_WARNING
#include <experimental/filesystem>

#include <cxxopts.hpp>
#include <stdiox.hpp>


#ifndef _WIN32
#include <sys/sendfile.h>
#endif


size_t g_read_chunk_size = 65536;


#ifdef _WIN32
inline
std::string get_socket_error_text() {
	return std::to_string(WSAGetLastError());
}
#else
std::string get_socket_error_text() {
	return std::string(std::strerror(errno)) + " (" + std::to_string(errno) + ")";
}
#endif

inline
int64_t get_time_millis() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

::sockaddr_in get_sockaddr_byname(const std::string& endpoint, int port)
{
	::sockaddr_in addr;
	::memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	{
		static std::mutex mutex;
		std::lock_guard<std::mutex> lock(mutex);
		::hostent* host = ::gethostbyname(endpoint.c_str());
		if(!host) {
			throw std::runtime_error("could not resolve: '" + endpoint + "'");
		}
		::memcpy(&addr.sin_addr.s_addr, host->h_addr_list[0], host->h_length);
	}
	return addr;
}

void recv_bytes(void* dst, const int fd, const size_t num_bytes)
{
	auto num_left = num_bytes;
	auto* dst_= (char*)dst;
	while(num_left > 0) {
		const auto num_read = ::recv(fd, dst_, num_left, 0);
		if(num_read < 0) {
			throw std::runtime_error("recv() failed with: " + get_socket_error_text());
		} else if(num_read == 0) {
			throw std::runtime_error("recv() failed with: EOF");
		}
		num_left -= num_read;
		dst_ += num_read;
	}
}

void send_bytes(const int fd, const void* src, const size_t num_bytes)
{
	if(::send(fd, (const char*)src, num_bytes, 0) != num_bytes) {
		throw std::runtime_error("send() failed with: " + get_socket_error_text());
	}
}

uint64_t send_file(const std::string& src_path, const std::string& dst_host, const int dst_port)
{
	FILE* src = fopen(src_path.c_str(), "rb");
	if(!src) {
		throw std::runtime_error("fopen() failed for " + src_path + " (" + std::string(strerror(errno)) + ")");
	}
	FSEEK(src, 0, SEEK_END);
	const uint64_t file_size = FTELL(src);
	FSEEK(src, 0, SEEK_SET);

	int fd = -1;
	uint64_t total_bytes = 0;
	try {
		fd = ::socket(AF_INET, SOCK_STREAM, 0);
		if(fd < 0) {
			throw std::runtime_error("socket() failed with: " + get_socket_error_text());
		}
		::sockaddr_in addr = get_sockaddr_byname(dst_host, dst_port);
		if(::connect(fd, (::sockaddr*)&addr, sizeof(addr)) < 0) {
			throw std::runtime_error("connect() failed with: " + get_socket_error_text());
		}
		send_bytes(fd, &file_size, 8);
		{
			char ret = -1;
			recv_bytes(&ret, fd, 1);
			if(ret != 1) {
				if(ret == 0) {
					throw std::runtime_error("no space left on destination");
				} else {
					throw std::runtime_error("unknown error on destination");
				}
			}
		}
		std::string file_name;
		{
			const auto pos = src_path.find_last_of("/\\");
			if(pos != std::string::npos) {
				file_name = src_path.substr(pos + 1);
			} else {
				file_name = src_path;
			}
		}
		{
			const uint16_t name_len = file_name.size();
			send_bytes(fd, &name_len, 2);
			send_bytes(fd, file_name.data(), name_len);
		}

#ifdef _WIN32
		std::vector<uint8_t> buffer(g_read_chunk_size * 16);
		while(true) {
			const auto num_bytes = fread(buffer.data(), 1, buffer.size(), src);
			send_bytes(fd, buffer.data(), num_bytes);
			total_bytes += num_bytes;
			if(num_bytes < buffer.size()) {
				break;
			}
		}
#else
		while(true) {
			const auto num_bytes = ::sendfile(fd, fileno(src), NULL, g_read_chunk_size * 1024);
			if(num_bytes < 0) {
				throw std::runtime_error("sendfile() failed with: " + get_socket_error_text());
			}
			total_bytes += num_bytes;
			if(num_bytes < g_read_chunk_size * 1024) {
				break;
			}
		}
#endif
	} catch(...) {
		CLOSESOCKET(fd);
		fclose(src);
		throw;
	}
	CLOSESOCKET(fd);
	fclose(src);

	return total_bytes;
}


int main(int argc, char** argv) try
{
#ifdef _WIN32
	{
		WSADATA data;
		const int wsaret = WSAStartup(MAKEWORD(1, 1), &data);
		if(wsaret != 0) {
			std::cerr << "WSAStartup() failed with error: " << wsaret << "\n";
			exit(-1);
		}
	}
#endif

	cxxopts::Options options("chia_plot_copy",
		"Copy plots via TCP to a chia_plot_sink.\n\n"
		"Usage: chia_plot_copy -t <host> -- *.plot ...\n"
	);

	int port = 1337;
	int threads = 10;
	bool do_remove = false;
	std::string target = "localhost";
	std::vector<std::string> file_list;

	options.allow_unrecognised_options().add_options()(
		"p, port", "Port to connect to (default = 1337)", cxxopts::value<int>(port))(
		"d, delete", "Delete files after copy (default = false)", cxxopts::value<bool>(do_remove))(
		"t, target", "Target hostname / IP address (default = localhost)", cxxopts::value<std::string>(target))(
		"r, nthreads", "Number of threads (default = 10)", cxxopts::value<int>(threads))(
		"f, files", "List of plot files", cxxopts::value<std::vector<std::string>>(file_list))(
		"help", "Print help");

	options.parse_positional("files");

	const auto args = options.parse(argc, argv);

	if(args.count("help") || file_list.empty()) {
		std::cout << options.help({""}) << std::endl;
		return 0;
	}

	std::mutex mutex;

#pragma omp parallel for num_threads(threads)
	for(int i = 0; i < int(file_list.size()); ++i)
	{
		const auto file_name = file_list[i];
		const auto time_begin = get_time_millis();
		{
			std::lock_guard<std::mutex> lock(mutex);
			std::cout << "Starting to copy " << file_name << " ..." << std::endl;
		}
		try {
			const auto num_bytes = send_file(file_name, target, port);

			const auto elapsed = (get_time_millis() - time_begin) / 1e3;
			{
				std::lock_guard<std::mutex> lock(mutex);
				std::cout << "Finished copy of " << file_name
						<< " (" << num_bytes / 1024 / 1024 / 1024. << " GiB) took " << elapsed << " sec, "
						<< num_bytes / pow(1024, 2) / elapsed << " MB/s" << std::endl;
			}
			if(do_remove) {
				std::remove(file_name.c_str());
			}
		}
		catch(const std::exception& ex) {
			std::lock_guard<std::mutex> lock(mutex);
			std::cout << "Failed to copy " << file_name << ": " << ex.what() << std::endl;
		}
	}
	
#ifdef _WIN32
	WSACleanup();
#endif
	return 0;
}
catch(const std::exception& ex) {
	std::cerr << "Failed with: " << ex.what() << std::endl;
}
