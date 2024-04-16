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
#include <set>
#include <chrono>
#include <csignal>
#include <cmath>
#include <random>
#include <algorithm>
#include <condition_variable>

#define _SILENCE_EXPERIMENTAL_FILESYSTEM_DEPRECATION_WARNING
#include <experimental/filesystem>

#include <cxxopts.hpp>
#include <stdiox.hpp>

#ifndef _WIN32
#include <poll.h>
#include <mad/DirectFile.h>
#endif


static std::string g_addr = "0.0.0.0";
static int g_port = 1337;
static int g_server = -1;
static bool g_do_run = true;
static bool g_force_shutdown = false;
static int g_recv_timeout_sec = 100;

static std::mutex g_mutex;
static std::condition_variable g_signal;
static std::map<uint64_t, std::shared_ptr<std::thread>> g_threads;
static std::map<std::string, uint64_t> g_reserved;
static std::map<std::string, int64_t> g_num_active;
static std::set<std::string> g_failed_drives;


inline
int64_t get_time_millis() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

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
void set_socket_nonblocking(int fd)
{
#ifdef _WIN32
	u_long mode = 1;
	const auto res = ::ioctlsocket(fd, FIONBIO, &mode);
	if(res != 0){
		throw std::runtime_error("ioctlsocket() failed with: " + get_socket_error_text());
	}
#else
	const auto res = ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
	if(res < 0) {
		throw std::runtime_error("fcntl() failed with: " + get_socket_error_text());
	}
#endif
}

inline
void set_socket_options(int sock, int send_buffer_size, int receive_buffer_size)
{
	if(send_buffer_size > 0 && setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (char*)&send_buffer_size, sizeof(send_buffer_size)) < 0) {
		throw std::runtime_error("setsockopt(SO_SNDBUF) failed with: " + get_socket_error_text());
	}
	if(receive_buffer_size > 0 && setsockopt(sock, SOL_SOCKET, SO_RCVBUF, (char*)&receive_buffer_size, sizeof(receive_buffer_size)) < 0) {
		throw std::runtime_error("setsockopt(SO_RCVBUF) failed with: " + get_socket_error_text());
	}
}

inline
int poll_fd_ex(const int fd, const int events, const int timeout_ms)
{
	::pollfd entry = {};
	entry.fd = fd;
	entry.events = events;
#ifdef _WIN32
	const auto ret = WSAPoll(&entry, 1, timeout_ms);
	if(ret == SOCKET_ERROR) {
		throw std::runtime_error("WSAPoll() failed with: " + get_socket_error_text());
	}
#else
	const auto ret = ::poll(&entry, 1, timeout_ms);
	if(ret < 0) {
		throw std::runtime_error("poll() failed with: " + get_socket_error_text());
	}
#endif
	return ret;
}

inline
void recv_bytes(void* dst, const int fd, const size_t num_bytes)
{
	auto num_left = num_bytes;
	auto* dst_= (char*)dst;
	while(num_left > 0) {
		const auto num_read = ::recv(fd, dst_, num_left, 0);
		if(num_read < 0) {
			throw std::runtime_error("recv() failed with: " + std::string(strerror(errno)));
		} else if(num_read == 0) {
			throw std::runtime_error("recv() failed with: EOF");
		}
		num_left -= num_read;
		dst_ += num_read;
	}
}

inline
void send_bytes(const int fd, const void* src, const size_t num_bytes)
{
	if(::send(fd, (const char*)src, num_bytes, 0) != num_bytes) {
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
	g_signal.notify_all();

	const int sock = ::socket(AF_INET, SOCK_STREAM, 0);
	::sockaddr_in addr = get_sockaddr_byname("localhost", g_port);
	::connect(sock, (::sockaddr*)&addr, sizeof(addr));
	CLOSESOCKET(sock);
}

static
void copy_func(const uint64_t job, const int fd, const uint64_t num_bytes, const std::string& dst_path, const std::string& file_name)
{
	const auto file_path = dst_path + (!dst_path.empty() && dst_path.back() != '/' ? "/" : "") + file_name;
	const auto tmp_file_path = file_path + ".tmp";

	bool is_drive_fail = false;

#ifdef _WIN32
	auto file = fopen(tmp_file_path.c_str(), "wb");
	if(!file) {
		std::lock_guard<std::mutex> lock(g_mutex);
		std::cerr << "fopen('" << tmp_file_path << "') failed with: " << strerror(errno) << std::endl;
	}
#else
	std::shared_ptr<mad::DirectFile> file;
	try {
		file = std::make_shared<mad::DirectFile>(tmp_file_path, false, true, true);
		file->sequential_write = true;
	} catch(const std::exception& ex) {
		std::lock_guard<std::mutex> lock(g_mutex);
		std::cerr << "open('" << tmp_file_path << "') failed with: " << ex.what() << std::endl;
	}
#endif

	if(file) {
		std::lock_guard<std::mutex> lock(g_mutex);
		std::cout << "Started copy to " << file_path << " (" << num_bytes / pow(1024, 3) << " GiB)" << std::endl;
	} else {
		is_drive_fail = true;
	}
	const auto time_begin = get_time_millis();

	uint64_t num_left = num_bytes;
	std::vector<uint8_t> buffer(16 * 1024 * 1024);

#ifndef _WIN32
	uint64_t offset = 0;
	mad::DirectFile::buffer_t write_buffer;
#endif

	try {
		set_socket_options(fd, 0, buffer.size());
	}
	catch(const std::exception& ex) {
		std::lock_guard<std::mutex> lock(g_mutex);
		std::cerr << ex.what() << std::endl;
	}
	set_socket_nonblocking(fd);

	while(file && num_left)
	{
		if(!poll_fd_ex(fd, POLLIN, g_recv_timeout_sec * 1000))
		{
			std::lock_guard<std::mutex> lock(g_mutex);
			std::cerr << "recv() failed with: timeout" << std::endl;
			break;
		}
		const auto num_read = ::recv(fd, buffer.data(), std::min<uint64_t>(num_left, buffer.size()), 0);
		if(num_read < 0) {
			std::lock_guard<std::mutex> lock(g_mutex);
			std::cerr << "recv() failed with: " << strerror(errno) << std::endl;
			break;
		} else if(num_read == 0) {
			std::lock_guard<std::mutex> lock(g_mutex);
			std::cerr << "recv() failed with: EOF" << std::endl;
			break;
		}
		num_left -= num_read;

#ifdef _WIN32
		if(fwrite(buffer.data(), 1, num_read, file) != num_read)
		{
			std::lock_guard<std::mutex> lock(g_mutex);
			std::cerr << "fwrite('" << tmp_file_path << "') failed with: " << strerror(errno) << std::endl;
			is_drive_fail = true;
			break;
		}
#else
		try {
			file->write(buffer.data(), num_read, offset, write_buffer);
		} catch(const std::exception& ex) {
			std::lock_guard<std::mutex> lock(g_mutex);
			std::cerr << "write('" << tmp_file_path << "') failed with: " << ex.what() << std::endl;
			is_drive_fail = true;
			break;
		}
		offset += num_read;
#endif
	}
	CLOSESOCKET(fd);

	if(file) {
#ifdef _WIN32
		if(fclose(file)) {
			std::lock_guard<std::mutex> lock(g_mutex);
			std::cerr << "fclose('" << tmp_file_path << "') failed with: " << strerror(errno) << std::endl;
			is_drive_fail = true;
		}
#else
		try {
			file->close();
		} catch(const std::exception& ex) {
			std::lock_guard<std::mutex> lock(g_mutex);
			std::cerr << "close('" << tmp_file_path << "') failed with: " << ex.what() << std::endl;
			is_drive_fail = true;
		}
#endif
	}

	if(num_left) {
		std::remove(tmp_file_path.c_str());
		std::lock_guard<std::mutex> lock(g_mutex);
		std::cerr << "Deleted " << tmp_file_path << std::endl;
	} else {
		if(std::rename(tmp_file_path.c_str(), file_path.c_str())) {
			std::lock_guard<std::mutex> lock(g_mutex);
			std::cerr << "rename('" << tmp_file_path << "') failed with: " << strerror(errno) << std::endl;
		}
	}
	{
		std::lock_guard<std::mutex> lock(g_mutex);
		if(auto thread = g_threads[job]) {
			thread->detach();
		}
		g_threads.erase(job);

		if(is_drive_fail) {
			g_failed_drives.insert(dst_path);
		}
		g_reserved[dst_path] -= num_bytes;
		g_num_active[dst_path]--;

		if(!num_left) {
			const auto elapsed = (get_time_millis() - time_begin) / 1e3;
			std::cout << "Finished copy to " << file_path << ", took " << elapsed << " sec, "
					<< num_bytes / pow(1024, 2) / elapsed << " MB/s" << std::endl;
		}
	}
	g_signal.notify_all();
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
	
	std::signal(SIGINT, trigger_shutdown);
	std::signal(SIGTERM, trigger_shutdown);
#ifndef _WIN32
	std::signal(SIGPIPE, SIG_IGN);
#endif

	cxxopts::Options options("chia_plot_sink",
		"Final copy engine to receive plots from one or more plotters via TCP and distribute to multiple disks in parallel.\n\n"
		"Usage: chia_plot_sink -- /mnt/disk0/ /mnt/disk1/ ...\n"
	);

	int max_num_active = 1;
	std::vector<std::string> dir_list;

	options.allow_unrecognised_options().add_options()(
		"B, address", "Address to listen on (default = 0.0.0.0)", cxxopts::value<std::string>(g_addr))(
		"p, port", "Port to listen on (default = 1337)", cxxopts::value<int>(g_port))(
		"T, timeout", "Receive timeout [sec] (default = 100)", cxxopts::value<int>(g_recv_timeout_sec))(
		"r, parallel", "Maximum number of parallel copies to same drive (default = 1, infinite = -1)", cxxopts::value<int>(max_num_active))(
		"d, destination", "List of destination folders", cxxopts::value<std::vector<std::string>>(dir_list))(
		"help", "Print help");

	options.parse_positional("destination");

	const auto args = options.parse(argc, argv);

	if(args.count("help") || dir_list.empty()) {
		std::cout << options.help({""}) << std::endl;
		return 0;
	}
	for(const auto& dir : dir_list) {
		std::cout << "Final Directory: " << dir << " (" << int(std::experimental::filesystem::space(dir).available / pow(1024, 3)) << " GiB free)" << std::endl;
	}

	// create server socket
	g_server = ::socket(AF_INET, SOCK_STREAM, 0);
	if(g_server < 0) {
		throw std::runtime_error("socket() failed with: " + get_socket_error_text());
	}
	{
		int enable = 1;
		if(::setsockopt(g_server, SOL_SOCKET, SO_REUSEADDR, (char*)&enable, sizeof(int)) < 0) {
			std::cerr << "setsockopt(SO_REUSEADDR) failed with: " << get_socket_error_text() << std::endl;
		}
	}
	{
		::sockaddr_in addr = get_sockaddr_byname(g_addr, g_port);
		if(::bind(g_server, (::sockaddr*)&addr, sizeof(addr)) < 0) {
			throw std::runtime_error("bind() failed with: " + get_socket_error_text());
		}
	}
	if(::listen(g_server, 1000) < 0) {
		throw std::runtime_error("listen() failed with: " + get_socket_error_text());
	}
	std::cout << "Listening on " << g_addr << ":" << g_port << std::endl;

	uint64_t job_counter = 0;
	std::default_random_engine rand_engine;

	while(g_do_run)
	{
		const int fd = ::accept(g_server, 0, 0);
		if(!g_do_run) {
			CLOSESOCKET(fd);
			break;
		}
		if(fd >= 0) {
			try {
				uint64_t file_size = 0;
				recv_bytes(&file_size, fd, 8);

				size_t wait_counter = 0;
				std::shared_ptr<std::string> out;
				while(!out) {
					std::unique_lock<std::mutex> lock(g_mutex);

					// first get drives which have no active copy operations
					std::vector<std::pair<std::string, uint64_t>> dirs;
					for(const auto& dir : dir_list) {
						if(!g_failed_drives.count(dir) && g_num_active[dir] == 0) {
							try {
								const auto available = std::experimental::filesystem::space(dir).available;
								if(available > 0) {
									dirs.emplace_back(dir, available);
								}
							} catch(const std::exception& ex) {
								std::cout << "Failed to get free space for " << dir << " (" << ex.what() << ")" << std::endl;
							}
						}
					}
					// sort by free space
					std::sort(dirs.begin(), dirs.end(),
						[](const std::pair<std::string, uint64_t>& L, const std::pair<std::string, uint64_t>& R) -> bool {
							return L.second > R.second;
						});

					// append drives which are already busy
					{
						std::vector<std::pair<std::string, uint64_t>> tmp;
						for(const auto& dir : dir_list) {
							const auto num_active = g_num_active[dir];
							if(!g_failed_drives.count(dir) && num_active > 0 && (num_active < max_num_active || max_num_active < 0)) {
								try {
									const auto available = std::experimental::filesystem::space(dir).available;
									if(available > 0) {
										tmp.emplace_back(dir, available);
									}
								} catch(...) {
									// ignore
								}
							}
						}
						std::sort(tmp.begin(), tmp.end(),
							[](const std::pair<std::string, uint64_t>& L, const std::pair<std::string, uint64_t>& R) -> bool {
								return g_num_active[L.first] < g_num_active[R.first];
							});
						dirs.insert(dirs.end(), tmp.begin(), tmp.end());
					}

					// select a drive with enough space available
					for(const auto& entry : dirs)
					{
						const auto& dir = entry.first;
						if(entry.second > g_reserved[dir] + file_size + 4096)
						{
							const auto prefix = dir + char(std::experimental::filesystem::path::preferred_separator);
							try {
								// check if this folder is disabled
								if(		!std::experimental::filesystem::exists(prefix + "chia_plot_sink_disable")
									&&	!std::experimental::filesystem::exists(prefix + "chia_plot_sink_disable.txt"))
								{
									out = std::make_shared<std::string>(dir);
									break;
								}
							} catch(...) {
								// ignore
							}
						}
					}
					if(!out) {
						if(!wait_counter++) {
							std::cout << "Waiting for previous copy to finish or more space to become available ..." << std::endl;
						}
						g_signal.wait_for(lock, std::chrono::seconds(1));
					}
					if(!g_do_run) {
						break;
					}
				}
				if(!g_do_run) {
					CLOSESOCKET(fd);
					break;
				}
				{
					const char cmd = 1;
					send_bytes(fd, &cmd, 1);
				}
				uint16_t file_name_len = 0;
				recv_bytes(&file_name_len, fd, 2);

				std::vector<char> file_name(file_name_len);
				recv_bytes(file_name.data(), fd, file_name_len);

				const auto dst_path = *out;
				{
					std::lock_guard<std::mutex> lock(g_mutex);
					g_reserved[dst_path] += file_size;
					g_num_active[dst_path]++;
					g_threads[job_counter] = std::make_shared<std::thread>(&copy_func,
							job_counter, fd, file_size, dst_path, std::string(file_name.data(), file_name.size()));
				}
				job_counter++;
			}
			catch(const std::exception& ex) {
				CLOSESOCKET(fd);
				std::lock_guard<std::mutex> lock(g_mutex);
				std::cerr << "accept() failed with: " << ex.what() << std::endl;
			}
		} else {
			std::cerr << "accept() failed with: " << get_socket_error_text() << std::endl;
			break;
		}
	}
	CLOSESOCKET(g_server);

	{
		std::unique_lock<std::mutex> lock(g_mutex);
		if(!g_threads.empty()) {
			std::cout << "Waiting for jobs to finish ..." << std::endl;
		}
		while(!g_threads.empty()) {
			g_signal.wait(lock);
		}
	}
	for(const auto& path : g_failed_drives) {
		std::cout << "Failed drive: " << path << std::endl;
	}
#ifdef _WIN32
	WSACleanup();
#endif
	return 0;
}
catch(const std::exception& ex) {
	std::cerr << "Failed with: " << ex.what() << std::endl;
}
