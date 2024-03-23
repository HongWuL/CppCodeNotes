#include <vector>
#include <unordered_map>
#include <thread>
#include <iostream>
#include <mutex>
#include <atomic>
#include <unistd.h>
#include "doubly_buffered_data.h"

typedef uint32_t ServerId;

std::mutex g_log_mtx;        //mutex for print
std::atomic<int> g_count;    //server count

class RoundRobinLoadBalancer {
public:
    ~RoundRobinLoadBalancer();
    bool AddServer(const ServerId& id);
    bool RemoveServer(const ServerId& id);
    int SelectServer(ServerId* out);
private:
    struct TLS {
        TLS(): offset(0), stride(0) {}
        int offset;
        int stride;
    };
    struct Servers {
        std::vector<ServerId> server_list;
        std::unordered_map<ServerId, size_t> server_map;
    };

    static bool Add(Servers& bg, const ServerId& id);
    static bool Remove(Servers& bg, const ServerId& id);

    DoublyBufferedData<Servers, TLS> _db_servers;
} g_lb; 

RoundRobinLoadBalancer::~RoundRobinLoadBalancer() {
}

bool RoundRobinLoadBalancer::Add(Servers& bg, const ServerId& id) {
    //这个是包装在Modify里的，是线程安全的
    if (bg.server_list.capacity() < 128) {
        // 提前扩容优化
        bg.server_list.reserve(128);
    }
    // 快速查找当前id是否已经存在
    std::unordered_map<ServerId, size_t>::iterator it = bg.server_map.find(id);
    if (it != bg.server_map.end()) {
        // 已经存在
        return false;
    }
    // 不存在则插入
    bg.server_map[id] = bg.server_list.size();
    bg.server_list.push_back(id);
    return true;
}

bool RoundRobinLoadBalancer::Remove(Servers& bg, const ServerId& id) {
    //这个是包装在Modify里的，是线程安全的
    std::unordered_map<ServerId, size_t>::iterator it = bg.server_map.find(id);
    if (it != bg.server_map.end()) {
        // 将最后一个节点和index节点交换，删除最后一个节点
        const size_t index = it->second;
        bg.server_list[index] = bg.server_list.back();
        bg.server_map[bg.server_list[index]] = index;
        bg.server_list.pop_back();
        bg.server_map.erase(it);
        return true;
    }
    return false;
}

bool RoundRobinLoadBalancer::AddServer(const ServerId& id) {
    return _db_servers.Modify(Add, id);
}

bool RoundRobinLoadBalancer::RemoveServer(const ServerId& id) {
    return _db_servers.Modify(Remove, id);
}

int RoundRobinLoadBalancer::SelectServer(ServerId* out) {
    //这个函数是热点函数，需要
    DoublyBufferedData<Servers, TLS>::ScopedPtr s;
    if (_db_servers.Read(&s) != 0) {
        return -1;
    }
    const size_t n = s->server_list.size();
    if (n == 0) {
        return -1;
    }
 
    TLS tls = s.tls();
    if (tls.stride == 0) {
        tls.stride = 1;
        tls.offset = rand() % n;
    }

    for (size_t i = 0; i < n; ++i) {
        tls.offset = (tls.offset + tls.stride) % n;
        *out = s->server_list[tls.offset];
        s.tls() = tls;
        return 0;
    }
    s.tls() = tls;
    return -1;
}

void RunSelect() {
    // 模拟选择server
    do {
        ServerId out;
        int rc = g_lb.SelectServer(&out);
        if(rc != 0) {
            std::lock_guard<std::mutex> lock(g_log_mtx);
            std::cout << "[ERROR]Select failed in thread " << std::this_thread::get_id() << std::endl;
        } else {
            std::lock_guard<std::mutex> lock(g_log_mtx);
            std::cout << "Thread " << std::this_thread::get_id() << " select server " << out << std::endl;
        }
        sleep(1);
    } while(1);
}

void RunAdd() {
    // 模拟添加server
    do {
        int id = rand() % 10;
        if(!g_lb.AddServer(id)) {
            std::lock_guard<std::mutex> lock(g_log_mtx);
            std::cout << "[ERROR]Add server "<< id << " failed in thread " << std::this_thread::get_id() << std::endl;
        } else {
            g_count ++;
            std::lock_guard<std::mutex> lock(g_log_mtx);
            std::cout << "Thread " << std::this_thread::get_id() << " add server " << id << std::endl;
        }
        if(g_count.load() > 5) {
            sleep(5);
        }
        sleep(1);
    } while(1);
}

void RunRemove() {
    // 模拟移除server
    do {
        int id = rand() % 10;
        if(!g_lb.RemoveServer(id)) {
            std::lock_guard<std::mutex> lock(g_log_mtx);
            std::cout << "[ERROR]Remove server "<< id <<" failed in thread " << std::this_thread::get_id() << std::endl;
        } else {
            g_count --;
            std::lock_guard<std::mutex> lock(g_log_mtx);
            std::cout << "Thread " << std::this_thread::get_id() << " remove server "<< id << std::endl;
        }
        if(g_count.load() <= 5) {
            sleep(5);
        }
        sleep(1);
    } while(1);
}

int main(int argc,char* argv[]) {
    std::vector<std::thread> thrs;
    thrs.emplace_back(RunAdd);
    thrs.emplace_back(RunRemove);
    sleep(1);
    for(int i = 0; i < 5; ++i) {
        thrs.emplace_back(RunSelect);
    }

    for(int i = 0; i < thrs.size(); ++i) {
        thrs[i].join();
    }
    return 0;

}