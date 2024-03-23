#include <vector>
#include <atomic>
#include <mutex>
#include <pthread.h>
#include <type_traits>
#include <iostream>
#include <memory>

class Void { };


template <typename T, typename TLS = Void>  //默认TLS为空类(不设置为NULL是为了支持&TLS)
class DoublyBufferedData {
    class Wrapper;
public:
    class ScopedPtr {
    //这个类包含指指针常量，指向存储的数据
    friend class DoublyBufferedData;
    public:
        ScopedPtr() : _data(NULL), _w(NULL) {}
        ~ScopedPtr() {
            if (_w) {
                _w->EndRead();
            }
        }
        const T* get() const { return _data; }
        const T& operator*() const { return *_data; }
        const T* operator->() const { return _data; }
        TLS& tls() { return _w->user_tls(); }   //返回TLS数据
        
    private:
        const T* _data; //所有线程共享的数据
        Wrapper* _w;    //Thread local数据
    };

    DoublyBufferedData();
    ~DoublyBufferedData();
    
    int Read(ScopedPtr* ptr);   //读出DBD中存储的_data指针和当前线程的Wrapper指针
    
    //修改后台数据
    template <typename Fn> size_t Modify(Fn& fn);
    template <typename Fn, typename Arg1> size_t Modify(Fn& fn, const Arg1&);

    //修改后台数据，但需要使用前台数据的视野来修改后台)
    template <typename Fn> size_t ModifyWithForeground(Fn& fn);
    template <typename Fn, typename Arg1> size_t ModifyWithForeground(Fn& fn, const Arg1&);

private:
    //辅助模板类。用于修改包含前台数据的闭包，0个参数
    template <typename Fn>
    struct WithFG0 {
        WithFG0(Fn& fn, T* data) : _fn(fn), _data(data) { }
        size_t operator()(T& bg) {
            //(const T&)_data[&bg == _data]用于提取前台fg数据
            //_data和_data[0]地址是一样的，如果_data[0]为bg，则结果为 _data[1]为fg; 反之_data[0]为fg
            //注意传入的是const，并不会修改前台数据，只是用来辅助修改后台数据
            return _fn(bg, (const T&)_data[&bg == _data]);  
        }
    private:
        Fn& _fn;    //实际修改函数
        T* _data;   //当前双缓冲区指针
    };
    
    //辅助模板类。用于修改包含前台数据的闭包，带1个参数

    template <typename Fn, typename Arg1>
    struct WithFG1 {
        WithFG1(Fn& fn, T* data, const Arg1& arg1)
            : _fn(fn), _data(data), _arg1(arg1) {}
        size_t operator()(T& bg) {
            return _fn(bg, (const T&)_data[&bg == _data], _arg1);
        }
    private:
        Fn& _fn;
        T* _data;
        const Arg1& _arg1;
    };
    
    //辅助模板类，用于修改数据T& bg，带有1个参数
    template <typename Fn, typename Arg1>
    struct Closure1 {
        Closure1(Fn& fn, const Arg1& arg1) : _fn(fn), _arg1(arg1) {}
        size_t operator()(T& bg) { return _fn(bg, _arg1); }
    private:
        Fn& _fn;
        const Arg1& _arg1;
    };

private:
    //读fg
    const T* UnsafeRead() const
        { return _data + _index.load(std::memory_order_acquire); }



    // 前台fg数据和后台bg数据
    T _data[2];

    //标定前台数据，0或1
    std::atomic<int> _index;

    //Wrapper相关函数和数据结构
    Wrapper* AddWrapper();            //添加一个Wrapper
    void RemoveWrapper(Wrapper*);     //移除特定Wrapper

    // 通过_wrapper_key来访问thread-local的wrappers
    bool _created_key;
    pthread_key_t _wrapper_key;

    // 所有thread-local的wrapper实例
    std::vector<Wrapper*> _wrappers;

    // Sequence access to _wrappers.
    std::mutex _wrappers_mutex;

    // Sequence modifications.
    std::mutex _modify_mutex;

};

static const pthread_key_t INVALID_PTHREAD_KEY = (pthread_key_t)-1;

// T为双缓冲区中的数据类型，TLS为thread local缓存，它俩在设计上是互不影响的！
template <typename T, typename TLS>
class DoublyBufferedDataWrapperBase {
public:
    TLS& user_tls() { return _user_tls; }
protected:
    TLS _user_tls;
};

//偏特化定义Void Wrapper
template <typename T>
class DoublyBufferedDataWrapperBase<T, Void> {
};

//Wrapper的实现
//Wrapper是thread-local的，主要作用:
//(1) 存储TLS数据
//(2) 控制读写共享缓冲区时的行为
template <typename T, typename TLS>
class DoublyBufferedData<T, TLS>::Wrapper
    : public DoublyBufferedDataWrapperBase<T, TLS> {
friend class DoublyBufferedData;
public:
    explicit Wrapper(DoublyBufferedData* c) : _control(c) {
    }
    
    ~Wrapper() {
        if (_control != NULL) {
            _control->RemoveWrapper(this);
        }
    }

    // _mutex will be locked by the calling pthread and DoublyBufferedData.
    // Most of the time, no modifications are done, so the mutex is
    // uncontended and fast.
    // TODO，读加锁是为了啥
    inline void BeginRead() {
        _mutex.lock();
    }

    inline void EndRead() {
        _mutex.unlock();
    }

    inline void WaitReadDone() {
        // 等待读结束，以便切换fg和bg
        std::lock_guard<std::mutex> lock(_mutex);
    }
    
private:
    DoublyBufferedData* _control;  
    std::mutex _mutex;
};

// Called when thread initializes thread-local wrapper.
// 在DoublyBufferedData里添加一个thread-local的wrapper，这个函数当thread初始化时会被调用
template <typename T, typename TLS>
typename DoublyBufferedData<T, TLS>::Wrapper*
DoublyBufferedData<T, TLS>::AddWrapper() {
    std::unique_ptr<Wrapper> w(new (std::nothrow) Wrapper(this));
    if (NULL == w) {
        return NULL;
    }
    try {
        std::lock_guard<std::mutex> lock(_wrappers_mutex);
        _wrappers.push_back(w.get());
    } catch (std::exception& e) {
        return NULL;
    }
    return w.release();
}

// Called when thread quits.
// 移除当前thread的wrappers
template <typename T, typename TLS>
void DoublyBufferedData<T, TLS>::RemoveWrapper(
    typename DoublyBufferedData<T, TLS>::Wrapper* w) {
    if (NULL == w) {
        return;
    }
    std::lock_guard<std::mutex> lock(_wrappers_mutex);
    for (size_t i = 0; i < _wrappers.size(); ++i) {
        if (_wrappers[i] == w) {
            _wrappers[i] = _wrappers.back();
            _wrappers.pop_back();
            return;
        }
    }
}

template <typename T> void delete_object(void* arg) {
    delete static_cast<T*>(arg);
}

// DoublyBufferedData的构造函数
template <typename T, typename TLS>
DoublyBufferedData<T, TLS>::DoublyBufferedData()
    : _index(0)
    , _created_key(false)
    , _wrapper_key(0) {
    _wrappers.reserve(64);
    const int rc = pthread_key_create(&_wrapper_key, delete_object<Wrapper>);  //创造_wrapper_key，每个thread看到的_wrapper_key是不一样的，也就是thread_local pthread_key_t;
    if (rc != 0) {
        std::cout << "Fail to pthread_key_create: " << rc << std::endl;
    } else {
        _created_key = true;
    }
    // Initialize _data for some POD types. This is essential for pointer
    // types because they should be Read() as NULL before any Modify().
    // 如果T是一些POD类型，则默认初始化缓冲区
    if (std::is_integral<T>::value || std::is_floating_point<T>::value ||
        std::is_pointer<T>::value || std::is_member_function_pointer<T>::value) {
        _data[0] = T();
        _data[1] = T();
    }
}

template <typename T, typename TLS>
DoublyBufferedData<T, TLS>::~DoublyBufferedData() {
    // 析构函数，删除_wrapper_key和所有Wrapper
    // User is responsible for synchronizations between Read()/Modify() and
    // this function.
    if (_created_key) {
        pthread_key_delete(_wrapper_key);
    }
    
    {
        std::lock_guard<std::mutex> lock(_wrappers_mutex);
        for (size_t i = 0; i < _wrappers.size(); ++i) {
            _wrappers[i]->_control = NULL;  // hack: disable removal.
            delete _wrappers[i];
        }
        _wrappers.clear();
    }
}

template <typename T, typename TLS>
int DoublyBufferedData<T, TLS>::Read(
    typename DoublyBufferedData<T, TLS>::ScopedPtr* ptr) {
    //把fg的数据和TLS数据读取到ScopedPtr* ptr中
    if (!_created_key) {
        return -1;
    }
    Wrapper* w = static_cast<Wrapper*>(pthread_getspecific(_wrapper_key));    //读线程私有Wrapper*
    if (w != NULL) {
        // Wrapper已经创建
        w->BeginRead();
        ptr->_data = UnsafeRead();
        ptr->_w = w;
        return 0;
    }
    //新建一个Wrapper
    w = AddWrapper();
    if (w != NULL) {
        const int rc = pthread_setspecific(_wrapper_key, w);
        if (rc == 0) {
            w->BeginRead();
            ptr->_data = UnsafeRead();
            ptr->_w = w;
            return 0;
        }
    }
    return -1;
}

// 使用函数Fn修改数据
// 先修改fg数据，再交换fg和bg，然后修改新bg(旧fg)数据
template <typename T, typename TLS>
template <typename Fn>
size_t DoublyBufferedData<T, TLS>::Modify(Fn& fn) {
    // _modify_mutex sequences modifications. Using a separate mutex rather
    // than _wrappers_mutex is to avoid blocking threads calling
    // AddWrapper() or RemoveWrapper() too long. Most of the time, modifications
    // are done by one thread, contention should be negligible.
    std::lock_guard<std::mutex> lock(_modify_mutex);
    int bg_index = !_index.load(std::memory_order_relaxed);
    // background instance is not accessed by other threads, being safe to
    // modify.
    // 修改bg
    const size_t ret = fn(_data[bg_index]);
    if (!ret) {
        // 修改失败
        return 0;
    }

    // Publish, flip background and foreground.
    // The release fence matches with the acquire fence in UnsafeRead() to
    // make readers which just begin to read the new foreground instance see
    // all changes made in fn.
    // 切换前后台
    _index.store(bg_index, std::memory_order_release);
    bg_index = !bg_index;
    
    // Wait until all threads finishes current reading. When they begin next
    // read, they should see updated _index.
    // 等待每个旧的前台读完，认为是前台看到新的前台了
    {
        std::lock_guard<std::mutex> lock(_wrappers_mutex);
        for (size_t i = 0; i < _wrappers.size(); ++i) {
            _wrappers[i]->WaitReadDone();
        }
    }
    // 修改新后台(旧前台)
    const size_t ret2 = fn(_data[bg_index]);
    return ret2;
}

// Modify的包装函数，传入修改函数和参数进行修改
template <typename T, typename TLS>
template <typename Fn, typename Arg1>
size_t DoublyBufferedData<T, TLS>::Modify(Fn& fn, const Arg1& arg1) {
    Closure1<Fn, Arg1> c(fn, arg1);
    return Modify(c);
}

template <typename T, typename TLS>
template <typename Fn>
size_t DoublyBufferedData<T, TLS>::ModifyWithForeground(Fn& fn) {
    WithFG0<Fn> c(fn, _data);
    return Modify(c);
}

template <typename T, typename TLS>
template <typename Fn, typename Arg1>
size_t DoublyBufferedData<T, TLS>::ModifyWithForeground(Fn& fn, const Arg1& arg1) {
    WithFG1<Fn, Arg1> c(fn, _data, arg1);
    return Modify(c);
}
