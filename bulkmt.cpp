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
    ICommandProcessor(const std::string& aName)
        : mName(aName)
    {
    }
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
    std::string mName;
    Counters mCounters;
};

class CommandProcessor : public ICommandProcessor
{
public:
    CommandProcessor(const std::string& aName, const CommandProcessors& dependentCommandProcessors = CommandProcessors())
        : ICommandProcessor(aName)
        , mDependentCommandProcessors(dependentCommandProcessors)
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
    CommandProcessors mDependentCommandProcessors;
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

/// Starts one thread for each dependent CommandProcessor
class ThreadedCommandProcessor : public CommandProcessor
{
public:
    ThreadedCommandProcessor(const CommandProcessors& dependentCommandProcessors)
        : CommandProcessor("threaded", dependentCommandProcessors)
    {
#ifdef DEBUG_PRINT
        std::cout << "Thread " << mName << " started." << std::endl;
#endif
        for (size_t i = 0; i < dependentCommandProcessors.size(); ++i)
            mThreads.emplace_back(std::thread(ThreadProc, this, i));
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
        mCondition.notify_all();
    }

    Counters GetCounters() const override
    {
        std::lock_guard<std::mutex> lk(mCountersMutex);
        return mCounters;
    }

    void Stop() override
    {
        mDone = true;
        mCondition.notify_all();
        for (auto& thread : mThreads)
            thread.join();
    }

private:
    std::queue<CommandBatch> mQueue;
    std::condition_variable mCondition;
    std::mutex mQueueMutex;
    mutable std::mutex mCountersMutex;
    std::atomic<bool> mDone{false};
    CommandProcessors mDependentCommandProcessors;

    static void ThreadProc(ThreadedCommandProcessor* aProcessor, size_t aIndex)
    {
        while (!aProcessor->mDone)
        {
            std::unique_lock<std::mutex> lk(aProcessor->mQueueMutex);
            aProcessor->mCondition.wait(lk,
                [&]()
                {
                    return !aProcessor->mQueue.empty() || aProcessor->mDone;
                });
            aProcessor->ProcessQueue(lk, aIndex);
        }
    }

    void ProcessQueue(std::unique_lock<std::mutex>& lk, size_t aIndex)
    {
        std::queue<CommandBatch> queue;
        std::swap(mQueue, queue);
        lk.unlock();
        while (!queue.empty())
        {
            auto commandBatch = queue.front();
            queue.pop();            
            std::string output = "bulk: " + Join(commandBatch.mCommands);
            mDependentCommandProcessors[aIndex]->ProcessCommand({output, commandBatch.mTimestamp});
            {
                std::lock_guard<std::mutex> lk(mCountersMutex);
                ++mCounters.mBlockCounter;
                mCounters.mCommandCounter += commandBatch.Size();
            }
        }
    }

    std::vector<std::thread> mThreads;
};

class ConsoleInput : public CommandProcessor
{
public:
    ConsoleInput(const CommandProcessors& dependentCommandProcessors = CommandProcessors())
        : CommandProcessor("console_input", dependentCommandProcessors)
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
                for (auto dependentCommandProcessor : mDependentCommandProcessors)
                    dependentCommandProcessor->StartBlock();
            }
        }
        else if (command.mText == "}")
        {
            if (--mBlockDepth == 0)
            {
                for (auto dependentCommandProcessor : mDependentCommandProcessors)
                    dependentCommandProcessor->FinishBlock();
            }
        }
        else
        {
            for (auto dependentCommandProcessor : mDependentCommandProcessors)
                dependentCommandProcessor->ProcessCommand(command);
            ++mCounters.mCommandCounter;
        }
    }

private:
    int mBlockDepth{0};
};

class ConsoleOutput : public CommandProcessor
{
public:
    ConsoleOutput(const std::string& aName)
        : CommandProcessor(aName)
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
    ReportWriter(const std::string& aFilePrefix)
        : CommandProcessor(aFilePrefix)
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
        filename << mName << "bulk" << seconds << ".log";
        return filename.str();
    }
};

class BatchCommandProcessor : public CommandProcessor
{
public:
    BatchCommandProcessor(int bulkSize, const CommandProcessors& dependentCommandProcessors)
        : CommandProcessor("batch", dependentCommandProcessors)
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
        for (auto dependentCommandProcessor : mDependentCommandProcessors)
        {
            dependentCommandProcessor->ProcessBatch({mCommandBatch, mCommandBatch[0].mTimestamp});
        }
        ClearBatch();
    }

    int mBulkSize;
    bool mBlockForced;
    std::vector<Command> mCommandBatch;
};

void RunBulk(int bulkSize)
{
    auto reportWriter1 = std::make_shared<ReportWriter>("file1");
    auto reportWriter2 = std::make_shared<ReportWriter>("file2");
    CommandProcessors reportWriters = {reportWriter1, reportWriter2};
    auto reportWritersThreadedProcessor = std::make_shared<ThreadedCommandProcessor>(reportWriters);

    auto consoleOutput = std::make_shared<ConsoleOutput>("log");
    CommandProcessors consoleOutputs = {consoleOutput};
    auto consoleOutputThreadedProcessor = std::make_shared<ThreadedCommandProcessor>(consoleOutputs);

    CommandProcessors processors = {reportWritersThreadedProcessor, consoleOutputThreadedProcessor};
    auto batchCommandProcessor = std::make_shared<BatchCommandProcessor>(bulkSize, processors);
    CommandProcessors batchCommandProcessors = {batchCommandProcessor};
    ConsoleInput consoleInput(batchCommandProcessors);
    std::string text;
    //std::fstream in("/home/mgorshkov/Work/bulkmt/sample");
    //while (std::getline(in, text))
    while (std::getline(std::cin, text))
    {
        consoleInput.ProcessCommand(Command{text, std::chrono::system_clock::now()});
    }

    reportWritersThreadedProcessor->Stop();
    consoleOutputThreadedProcessor->Stop();

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
