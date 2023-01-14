/*
 * chia_plot_copy.cpp
 *
 *  Created on: Jan 14, 2023
 *      Author: mad
 */

#include <mutex>
#include <string>
#include <experimental/filesystem>

#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

#include <cxxopts.hpp>
#include <stdiox.hpp>


size_t g_read_chunk_size = 65536;


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
			throw std::runtime_error("recv() failed with: " + std::string(strerror(errno)));
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
		throw std::runtime_error("send() failed with: " + std::string(strerror(errno)));
	}
}

uint64_t send_file(const std::string& src_path, const std::string& dst_host, const int dst_port)
{
	FILE* src = fopen(src_path.c_str(), "rb");
	if(!src) {
		throw std::runtime_error("fopen() failed for " + src_path + " (" + std::string(strerror(errno)) + ")");
	}
	FSEEK(src, 0, SEEK_END);
	const uint64_t file_size = ftell(src);
	FSEEK(src, 0, SEEK_SET);

	int fd = -1;
	uint64_t total_bytes = 0;
	try {
		fd = ::socket(AF_INET, SOCK_STREAM, 0);
		if(fd < 0) {
			throw std::runtime_error("socket() failed with: " + std::string(strerror(errno)));
		}
		::sockaddr_in addr = get_sockaddr_byname(dst_host, dst_port);
		if(::connect(fd, (::sockaddr*)&addr, sizeof(addr)) < 0) {
			throw std::runtime_error("connect() failed with: " + std::string(strerror(errno)));
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

		std::vector<uint8_t> buffer(g_read_chunk_size * 16);
		while(true) {
			const auto num_bytes = fread(buffer.data(), 1, buffer.size(), src);
			send_bytes(fd, buffer.data(), num_bytes);
			total_bytes += num_bytes;
			if(num_bytes < buffer.size()) {
				break;
			}
		}
	} catch(...) {
		CLOSESOCKET(fd);
		fclose(src);
		throw;
	}
	CLOSESOCKET(fd);
	fclose(src);

	return total_bytes;
}


int main(int argc, char** argv)
{
	cxxopts::Options options("chia_plot_copy",
		"Copy plots via TCP to a chia_plot_sink.\n\n"
		"Usage: chia_plot_copy -t <host> -- *.plot ...\n"
	);

	int port = 1337;
	bool do_remove = false;
	std::string target = "localhost";
	std::vector<std::string> file_list;

	options.allow_unrecognised_options().add_options()(
		"p, port", "Port to connect to (default = 1337)", cxxopts::value<int>(port))(
		"d, delete", "Delete files after copy (default = false)", cxxopts::value<bool>(do_remove))(
		"t, target", "Target hostname / IP address (default = localhost)", cxxopts::value<std::string>(target))(
		"f, files", "List of plot files", cxxopts::value<std::vector<std::string>>(file_list))(
		"help", "Print help");

	options.parse_positional("files");

	const auto args = options.parse(argc, argv);

	if(args.count("help") || file_list.empty()) {
		std::cout << options.help({""}) << std::endl;
		return 0;
	}

	for(const auto& file_name : file_list)
	{
		const auto num_bytes = send_file(file_name, target, port);
		std::cout << "Sent " << file_name << " (" << num_bytes / 1024 / 1024. << " MiB)" << std::endl;

		if(do_remove) {
			std::remove(file_name.c_str());
		}
	}

	return 0;
}
