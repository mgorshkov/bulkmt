#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <chrono>
#include <condition_variable>
#include <thread>
#include <queue>
#include <memory>
#include <functional>
#include <atomic>

using namespace std::chrono_literals;

/**
 * @brief Multithreaded batch command processor.
 */

using Timestamp = std::chrono::system_clock::time_point;

struct Command
{
    std::string mText;
    Timestamp mTimestamp;
};

struct CommandBatch
{
    std::vector<Command> mCommands;
    Timestamp mTimestamp;

    size_t Size() const
    {
        return mCommands.size();
    }
};

struct Counters
{
    int mLineCounter{0};
    int mBlockCounter{0};
    int mCommandCounter{0};
};

class CommandProcessor;
using CommandProcessors = std::vector<std::shared_ptr<CommandProcessor>>;

class ICommandProcessor
{
public:
    virtual ~ICommandProcessor() = default;

    virtual void StartBlock() = 0;
    virtual void FinishBlock() = 0;
    virtual void ProcessCommand(const Command& command) = 0;
    virtual void ProcessBatch(const CommandBatch& commandBatch) = 0;

    virtual void Stop() = 0;

    virtual Counters GetCounters() const
    {
        return mCounters;
    }

protected:
    Counters mCounters;
};

class CommandProcessor : public ICommandProcessor
{
public:
    CommandProcessor(const CommandProcessors& nextCommandProcessors = CommandProcessors())
        : mNextCommandProcessors(nextCommandProcessors)
    {
    }

    virtual ~CommandProcessor() = default;

    virtual void StartBlock() {}
    virtual void FinishBlock() {}
    virtual void ProcessCommand(const Command& command) {}
    virtual void ProcessBatch(const CommandBatch& commandBatch) {}

    virtual void Stop() {}

    virtual Counters GetCounters() const
    {
        Counters counters{mCounters};

        /*for (auto commandProcessor : mNextCommandProcessors)
        {
            Counters processorCounters = commandProcessor->GetCounters(); 
            counters.mLineCounter += processorCounters.mLineCounter;
            counters.mBlockCounter += processorCounters.mBlockCounter;
            counters.mCommandCounter += processorCounters.mCommandCounter;
        }*/

        return counters;
    }

protected:
    CommandProcessors mNextCommandProcessors;
};

static std::string Join(const std::vector<Command>& v)
{
    std::stringstream ss;
    for(size_t i = 0; i < v.size(); ++i)
    {
        if(i != 0)
            ss << ", ";
        ss << v[i].mText;
    }
    return ss.str();
}

template <typename Processor>
class ThreadedCommandProcessor : public CommandProcessor
{
public:
    ThreadedCommandProcessor(const std::string& aName)
        : mInternalProcessor(aName)
        , mName(aName)
    {
#ifdef DEBUG_PRINT
        std::cout << "Thread " << mName << " started." << std::endl;
#endif
    }

    ~ThreadedCommandProcessor()
    {
        std::cout << "Thread: " << mName << ", blocks: " << mCounters.mBlockCounter << ", commands: " << mCounters.mCommandCounter << std::endl;
    }

    void ProcessBatch(const CommandBatch& commandBatch) override
    {
        if (mDone)
            return;
        {
            std::lock_guard<std::mutex> lk(mQueueMutex);
            mQueue.push(commandBatch);
        }
        mCond.notify_one();
    }

    Counters GetCounters() const override
    {
        std::lock_guard<std::mutex> lk(mCountersMutex);
        return mCounters;
    }

    void Stop() override
    {
#ifdef DEBUG_PRINT
        std::cout << "Thread " << mName << ": stopping." << std::endl;
#endif
        mDone = true;
        mCond.notify_one();
        mThread.join();
#ifdef DEBUG_PRINT
        std::cout << "Thread " << mName << ": stopped." << std::endl;
#endif
    }

private:
    std::queue<CommandBatch> mQueue;
    std::condition_variable mCond;
    std::mutex mQueueMutex;
    mutable std::mutex mCountersMutex;
    std::atomic<bool> mDone{false};
    Processor mInternalProcessor;
    std::string mName;

    void ProcessQueue(std::unique_lock<std::mutex>& lk)
    {
        std::queue<CommandBatch> queue;
        std::swap(mQueue, queue);
        lk.unlock();
        while (!queue.empty())
        {
            auto commandBatch = queue.front();
            queue.pop();            
            std::string output = "bulk: " + Join(commandBatch.mCommands);
            mInternalProcessor.ProcessCommand({output, commandBatch.mTimestamp});
            {
                std::lock_guard<std::mutex> lk(mCountersMutex);
                ++mCounters.mBlockCounter;
                mCounters.mCommandCounter += commandBatch.Size();
            }
#ifdef DEBUG_PRINT
            std::cout << "Thread " << mName << ": after process command." << std::endl;
#endif
        }
    }

    std::thread mThread{
        [&]()
        {
            while (!mDone)
            {
#ifdef DEBUG_PRINT
                std::cout << "Thread " << mName << ": before unique_lock." << std::endl;
#endif
                std::unique_lock<std::mutex> lk(mQueueMutex);
#ifdef DEBUG_PRINT
                std::cout << "Thread " << mName << ": after unique_lock." << std::endl;
#endif
                mCond.wait(lk,
                    [&]()
                    {
                        return !mQueue.empty() || mDone;
                    });
#ifdef DEBUG_PRINT
                std::cout << "Thread " << mName << ": after wait." << std::endl;                
#endif          
                ProcessQueue(lk);      
            }
        }
    };
};

class ConsoleInput : public CommandProcessor
{
public:
    ConsoleInput(const CommandProcessors& nextCommandProcessors = CommandProcessors())
        : CommandProcessor(nextCommandProcessors)
    {
    }

    ~ConsoleInput()
    {
    }

    void ProcessCommand(const Command& command) override
    {
        ++mCounters.mLineCounter;

        if (command.mText == "{")
        {
            if (++mBlockDepth > 0)
            {
                for (auto nextCommandProcessor : mNextCommandProcessors)
                    nextCommandProcessor->StartBlock();
            }
        }
        else if (command.mText == "}")
        {
            if (--mBlockDepth == 0)
            {
                for (auto nextCommandProcessor : mNextCommandProcessors)
                    nextCommandProcessor->FinishBlock();
            }
        }
        else
        {
            for (auto nextCommandProcessor : mNextCommandProcessors)
                nextCommandProcessor->ProcessCommand(command);
            ++mCounters.mCommandCounter;
        }
    }

private:
    int mBlockDepth{0};
};

class ConsoleOutput : public CommandProcessor
{
public:
    ConsoleOutput(const std::string&)
    {
    }

    void ProcessCommand(const Command& command) override
    {
        std::cout << command.mText << std::endl;
    }
};

class ReportWriter : public CommandProcessor
{
public:
    ReportWriter(const std::string& filePrefix)
        : mFilePrefix(filePrefix)
    {
    }

    void ProcessCommand(const Command& command) override
    {
        std::ofstream file(GetFilename(command), std::ofstream::out);
        file << command.mText << std::endl;
    }

private:
    std::string GetFilename(const Command& command)
    {
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(
        command.mTimestamp.time_since_epoch()).count();
        std::stringstream filename;
        filename << mFilePrefix << "bulk" << seconds << ".log";
        return filename.str();
    }

    std::string mFilePrefix;
};

class BatchCommandProcessor : public CommandProcessor
{
public:
    BatchCommandProcessor(int bulkSize, const CommandProcessors& nextCommandProcessors)
        : CommandProcessor(nextCommandProcessors)
        , mBulkSize(bulkSize)
        , mBlockForced(false)
    {
    }

    ~BatchCommandProcessor()
    {
        if (!mBlockForced)
            DumpBatch();
    }

    void StartBlock() override
    {
        mBlockForced = true;
        DumpBatch();
    }

    void FinishBlock() override
    {
        mBlockForced = false;
        DumpBatch();
    }

    void ProcessCommand(const Command& command) override
    {
        ++mCounters.mCommandCounter;
        mCommandBatch.push_back(command);

        if (!mBlockForced && mCommandBatch.size() >= mBulkSize)
        {
            DumpBatch();
        }
    }
private:
    void ClearBatch()
    {
        mCommandBatch.clear();
    }

    void DumpBatch()
    {
        if (mCommandBatch.empty())
            return;
        for (auto nextCommandProcessor : mNextCommandProcessors)
        {
            nextCommandProcessor->ProcessBatch({mCommandBatch, mCommandBatch[0].mTimestamp});
        }
        ClearBatch();
    }

    int mBulkSize;
    bool mBlockForced;
    std::vector<Command> mCommandBatch;
};

void RunBulk(int bulkSize)
{
    auto reportWriter1 = std::make_shared<ThreadedCommandProcessor<ReportWriter>>("file1");
    auto reportWriter2 = std::make_shared<ThreadedCommandProcessor<ReportWriter>>("file2");
    auto consoleOutput = std::make_shared<ThreadedCommandProcessor<ConsoleOutput>>("log");
    CommandProcessors processors = {reportWriter1, reportWriter2, consoleOutput};
    auto batchCommandProcessor = std::make_shared<BatchCommandProcessor>(bulkSize, processors);
    ConsoleInput consoleInput({batchCommandProcessor});
    std::string text;
    //std::fstream in("/home/mgorshkov/Work/bulkmt/sample");
    //while (std::getline(in, text))
    while (std::getline(std::cin, text))
    {
        consoleInput.ProcessCommand(Command{text, std::chrono::system_clock::now()});
    }

    reportWriter1->Stop();
    reportWriter2->Stop();
    consoleOutput->Stop();

    Counters consoleCounters = consoleInput.GetCounters();

    std::cout << "Thread: main, blocks: " << consoleCounters.mBlockCounter << 
        ", commands: " << consoleCounters.mCommandCounter << ", lines: " << consoleCounters.mLineCounter << std::endl;
}

int main(int argc, char const** argv)
{
    try
    {
        if (argc < 2)
        {
            std::cerr << "Bulk size is not specified." << std::endl;
            return 1;
        }

        int bulkSize = atoi(argv[1]);
        if (bulkSize == 0)
        {
            std::cerr << "Invalid bulk size." << std::endl;
            return 1;
        }

        RunBulk(bulkSize);
        return 0;
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}
