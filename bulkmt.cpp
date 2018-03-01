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
#include <assert.h>

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

class ICommandProcessor
{
public:
    virtual ~ICommandProcessor() = default;

    virtual void StartBlock() = 0;
    virtual void FinishBlock() = 0;

    virtual void ProcessLine(const std::string&) = 0;
    virtual void ProcessCommand(const Command&) = 0;
    virtual void ProcessBatch(const CommandBatch&) = 0;

    virtual void Stop() = 0;

    virtual void DumpCounters() const = 0;
};

using CommandProcessors = std::vector<std::shared_ptr<ICommandProcessor>>;

class CommandProcessor : public ICommandProcessor
{
public:
    CommandProcessor(const std::string& aName, const CommandProcessors& dependentCommandProcessors = CommandProcessors())
        : mName(aName)
        , mDependentCommandProcessors(dependentCommandProcessors)
    {
    }

    ~CommandProcessor()
    {
    }

    void StartBlock() override
    {
        for (auto dependentCommandProcessor : mDependentCommandProcessors)
            dependentCommandProcessor->StartBlock();

        ++mCounters.mBlockCounter;
    }

    void FinishBlock() override
    {
        for (auto dependentCommandProcessor : mDependentCommandProcessors)
            dependentCommandProcessor->FinishBlock();
    }

    void ProcessLine(const std::string& line) override
    {
        for (auto dependentCommandProcessor : mDependentCommandProcessors)
            dependentCommandProcessor->ProcessLine(line);

        ++mCounters.mLineCounter;
    }

    void ProcessCommand(const Command& command) override
    {
        for (auto dependentCommandProcessor : mDependentCommandProcessors)
            dependentCommandProcessor->ProcessCommand(command);

        ++mCounters.mCommandCounter;
    }

    void ProcessBatch(const CommandBatch& commandBatch) override
    {
        for (auto dependentCommandProcessor : mDependentCommandProcessors)
        {
            dependentCommandProcessor->ProcessBatch(commandBatch);
        }

        ++mCounters.mBlockCounter;
        mCounters.mCommandCounter += commandBatch.Size();
    }

    void Stop() override
    {
        DumpCounters();
        for (auto dependentCommandProcessor : mDependentCommandProcessors)
            dependentCommandProcessor->Stop();
    }

    void DumpCounters() const override
    {
        std::cout << "Thread: " << mName << ", blocks: " << mCounters.mBlockCounter <<
            ", commands: " << mCounters.mCommandCounter << ", lines: " << mCounters.mLineCounter << std::endl;
    }

protected:
    std::string mName;
    thread_local static Counters mCounters;

private:
    CommandProcessors mDependentCommandProcessors;
};

thread_local Counters CommandProcessor::mCounters;

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

static Command MakeCommandFromBatch(const CommandBatch& aCommandBatch)
{
    std::string output = "bulk: " + Join(aCommandBatch.mCommands);
    Command command{output, aCommandBatch.mTimestamp};
    return command;
}

/// Starts one thread for each dependent CommandProcessor
template <typename DependentProcessor>
class ThreadedCommandProcessor : public CommandProcessor
{
public:
    ThreadedCommandProcessor(const std::string& aName, int aThreads = 1)
        : CommandProcessor("main")
    {
        for (int i = 0; i < aThreads; ++i)
        {
            std::stringstream name;
            name << aName << i;
            mThreads.emplace_back(std::thread(ThreadProc, this, name.str()));
        }
    }

    ~ThreadedCommandProcessor()
    {
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
        CommandProcessor::ProcessBatch(commandBatch);
    }

    void Stop() override
    {
        mDone = true;
        mCondition.notify_all();
        for (auto& thread : mThreads)
            thread.join();
        CommandProcessor::Stop();
    }

private:
    static void ThreadProc(ThreadedCommandProcessor* aProcessor, const std::string& aName)
    {
        try
        {
            DependentProcessor dependentProcessor(aName);
            while (!aProcessor->mDone)
            {
                std::unique_lock<std::mutex> lk(aProcessor->mQueueMutex);
                aProcessor->mCondition.wait(lk,
                    [&]()
                    {
                        return !aProcessor->mQueue.empty() || aProcessor->mDone;
                    });
                aProcessor->ProcessQueue(lk, dependentProcessor);
            }
            dependentProcessor.Stop();
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << std::endl;
        }
    }

    void ProcessQueue(std::unique_lock<std::mutex>& lk, CommandProcessor& aDependentProcessor)
    {
        std::queue<CommandBatch> queue;
        std::swap(mQueue, queue);
        lk.unlock();
        while (!queue.empty())
        {
            auto commandBatch = queue.front();
            queue.pop();
            aDependentProcessor.ProcessBatch(commandBatch);
        }
    }

    std::queue<CommandBatch> mQueue;
    std::condition_variable mCondition;
    std::mutex mQueueMutex;
    std::atomic<bool> mDone{false};

    std::vector<std::thread> mThreads;
};

class ConsoleInput : public CommandProcessor
{
public:
    ConsoleInput(const CommandProcessors& dependentCommandProcessors)
        : CommandProcessor("main", dependentCommandProcessors)
    {
        assert(dependentCommandProcessors.size() != 0);
    }

    ~ConsoleInput()
    {
    }

    void ProcessLine(const std::string& line) override
    {
        if (line == "{")
        {
            if (++mBlockDepth > 0)
            {
                CommandProcessor::StartBlock();
            }
        }
        else if (line == "}")
        {
            if (--mBlockDepth == 0)
            {
                CommandProcessor::FinishBlock();
            }
        }
        else
        {
            Command command{line, std::chrono::system_clock::now()};
            CommandProcessor::ProcessCommand(command);
        }
        CommandProcessor::ProcessLine(line);
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

    ~ConsoleOutput()
    {
    }

    void ProcessBatch(const CommandBatch& commandBatch) override
    {
        Command command = MakeCommandFromBatch(commandBatch);

        std::cout << command.mText << std::endl;

        CommandProcessor::ProcessBatch(commandBatch);
    }
};

class ReportWriter : public CommandProcessor
{
public:
    ReportWriter(const std::string& aName)
        : CommandProcessor(aName)
    {
    }

    ~ReportWriter()
    {
    }

    void ProcessBatch(const CommandBatch& commandBatch) override
    {
        Command command = MakeCommandFromBatch(commandBatch);

        std::ofstream file(GetFilename(command), std::ofstream::out);
        file << command.mText << std::endl;

        CommandProcessor::ProcessBatch(commandBatch);
    }

private:
    std::string GetFilename(const Command& command)
    {
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(
        command.mTimestamp.time_since_epoch()).count();
        std::stringstream filename;
        filename << mName << Separator << "bulk" << Separator
            << mCounters.mBlockCounter << Separator << seconds << ".log";
        return filename.str();
    }

    static const char Separator = '-';
};

class BatchCommandProcessor : public CommandProcessor
{
public:
    BatchCommandProcessor(size_t bulkSize, const CommandProcessors& dependentCommandProcessors)
        : CommandProcessor("main", dependentCommandProcessors)
        , mBulkSize(bulkSize)
        , mBlockForced(false)
    {
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
        mCommandBatch.push_back(command);

        if (!mBlockForced && mCommandBatch.size() >= mBulkSize)
        {
            DumpBatch();
        }
    }

    void Stop() override
    {
        if (!mBlockForced)
            DumpBatch();
        CommandProcessor::Stop();
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
        CommandBatch commandBatch{mCommandBatch, mCommandBatch[0].mTimestamp};
        CommandProcessor::ProcessBatch(commandBatch);
        ClearBatch();
    }

    size_t mBulkSize;
    bool mBlockForced;
    std::vector<Command> mCommandBatch;
};

void RunBulk(int bulkSize)
{
    auto reportWritersThreadedProcessor = std::make_shared<ThreadedCommandProcessor<ReportWriter>>("file", 2);
    auto consoleOutputThreadedProcessor = std::make_shared<ThreadedCommandProcessor<ConsoleOutput>>("log");

    CommandProcessors processors = {reportWritersThreadedProcessor, consoleOutputThreadedProcessor};
    auto batchCommandProcessor = std::make_shared<BatchCommandProcessor>(bulkSize, processors);
    CommandProcessors batchCommandProcessors = {batchCommandProcessor};
    ConsoleInput consoleInput(batchCommandProcessors);
    std::string line;
    std::fstream stream("/home/mgorshkov/Work/bulkmt/CMakeCache.txt");
    //while (std::getline(std::cin, line))
    while (std::getline(stream, line))
    {
        consoleInput.ProcessLine(line);
    }
    consoleInput.Stop();
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
