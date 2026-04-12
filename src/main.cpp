#include "Server.hpp"
#include "Client.hpp"
#include "../libs/ds/concurrentqueue.h"
#include "../libs/utils.hpp"
#include <atomic>
#include <iostream>
#include <thread>
#include <signal.h>
#include <cstdio>

// [program] [my_index] [port] [n * ip addresses]
int main(const int argc, const char** argv){
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::setvbuf(stderr, nullptr, _IONBF, 0);

    if(argc < 5){
        std::cout << "Usage: " << argv[0] << " <index> <my_port> <ip:port1> <ip:port2> ... <ip:portN>\n";
        return -1;
    }

    int port = std::atoi(argv[2]);

    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    sigprocmask(SIG_BLOCK, &mask, nullptr);

    std::atomic<bool> running{true};

    moodycamel::ConcurrentQueue<Request_Cut> req_queue;
    moodycamel::ConcurrentQueue<Response_Cut> resp_queue;

    std::thread server_thread(runServer, port, argc, argv,
                              std::ref(req_queue), std::ref(resp_queue),
                              std::ref(running));
    std::thread client_thread(runClient,
                              std::ref(req_queue), std::ref(resp_queue),
                              std::ref(running));

    int sig;
    sigwait(&mask, &sig);

    running.store(false);

    server_thread.join();
    client_thread.join();

    return 0;
}
