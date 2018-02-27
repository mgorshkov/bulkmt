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

/**
 * @brief Multithreaded batch command processor.
 */

struct Command
{
    std::string Text;
    std::chrono::system_clock::time_point Timestamp;
};

class CommandProcessor;
using CommandProcessors = std::vector<std::shared_ptr<CommandProcessor>>;

class CommandProcessor
{
public:
    CommandProcessor(const CommandProcessors& nextCommandProcessors = CommandProcessors())
        : mNextCommandProcessors(nextCommandProcessors)
    {
    }

    virtual ~CommandProcessor() = default;

    virtual void StartBlock() {}
    virtual void FinishBlock() {}
    virtual void ProcessCommand(const Command& command) = 0;

protected:
    CommandProcessors mNextCommandProcessors;
};

template <typename P>
class ThreadedCommandProcessor : public CommandProcessor
{
public:
    ThreadedCommandProcessor(const std::string& aName)
        : mInternalProcessor(aName)
    {
    }

    void StartBlock() override
    {
        ++mBlockCounter;
    }


    void ProcessCommand(const Command& command) override
    {
        {
            std::lock_guard<std::mutex> lk(mMutex);
            mQueue.push(command);
            ++mCommandCounter;
        }
        mCond.notify_one();
    }

    void Stop()
    {
        mDone = false;
        mThread.join();

        std::cout << "Block counter: " << mBlockCounter << std::endl << ", command counter: " << mCommandCounter << std::endl;
    }

private:
    int mBlockCounter{0};
    int mCommandCounter{0};
    std::queue<Command> mQueue;
    std::condition_variable mCond;
    std::mutex mMutex;
    std::atomic<bool> mDone{false};
    P mInternalProcessor;

    std::thread mThread{
        [&]()
        {
            while (!mDone)
            {
                std::unique_lock<std::mutex> lk(mMutex);
                mCond.wait(lk,
                    [&]()
                    {
                        return !mQueue.empty();
                    });
                auto command = mQueue.front();
                mQueue.pop();
                lk.unlock();
                mInternalProcessor.ProcessCommand(command);
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

    void ProcessCommand(const Command& command) override
    {
        if (!mNextCommandProcessors.empty())
        {
            if (command.Text == "{")
            {
                if (++mBlockDepth > 0)
                {
                    for (auto nextCommandProcessor : mNextCommandProcessors)
                        nextCommandProcessor->StartBlock();
                }
            }
            else if (command.Text == "}")
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
            }
        }
    }

private:
    int mBlockCounter{0};
    int mCommandCounter{0};
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
        std::cout << command.Text << std::endl;
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
        file << command.Text;
    }

private:
    std::string GetFilename(const Command& command)
    {
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(
        command.Timestamp.time_since_epoch()).count();
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
        for (auto nextCommandProcessor : mNextCommandProcessors)
        {
            std::string output = "bulk: " + Join(mCommandBatch);
            nextCommandProcessor->ProcessCommand(Command{output, mCommandBatch[0].Timestamp});
        }
        ClearBatch();
    }

    static std::string Join(const std::vector<Command>& v)
    {
        std::stringstream ss;
        for(size_t i = 0; i < v.size(); ++i)
        {
            if(i != 0)
                ss << ", ";
            ss << v[i].Text;
        }
        return ss.str();
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
    while (std::getline(std::cin, text))
        consoleInput.ProcessCommand(Command{text, std::chrono::system_clock::now()});
    reportWriter1->Stop();
    reportWriter2->Stop();
    consoleOutput->Stop();
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
