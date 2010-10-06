require 'spec/spec_helper'

describe "Kestrel::Client::Transactional" do
   before do
     @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
     @kestrel = Kestrel::Client::Transactional.new(@raw_kestrel_client)
     @queue = "some_queue"
   end

   describe "integration" do
    def get_job
      job = nil
      job = @kestrel.get(@queue) until job
      job
    end

    it "processes normal jobs" do
      returns = [:mcguffin]
      stub(@raw_kestrel_client).get(@queue, anything) { returns.shift }
      stub(@raw_kestrel_client).get(@queue + "_errors", anything)
      mock(@raw_kestrel_client).get_from_last(@queue + "/close")

      get_job.should == :mcguffin
      @kestrel.current_try.should == 1
      @kestrel.get(@queue) # simulate next get run
    end

    it "processes successful retries" do
      returns = [Kestrel::Client::Transactional::RetryableJob.new(1, :mcguffin)]
      stub(@raw_kestrel_client).get(@queue + "_errors", anything) { returns.shift }
      stub(@raw_kestrel_client).get(@queue, anything)
      mock(@raw_kestrel_client).get_from_last(@queue + "_errors/close")

      get_job.should == :mcguffin
      @kestrel.current_try.should == 2
      @kestrel.get(@queue) # simulate next get run
    end

    it "processes normal jobs that should retry" do
      returns = [:mcguffin]
      stub(@raw_kestrel_client).get(@queue, anything) { returns.shift }
      stub(@raw_kestrel_client).get(@queue + "_errors", anything)
      mock(@raw_kestrel_client).set(@queue + "_errors", anything) do |q,j|
        j.retries.should == 1
        j.job.should == :mcguffin
      end
      mock(@raw_kestrel_client).get_from_last(@queue + "/close")

      get_job.should == :mcguffin
      @kestrel.current_try.should == 1

      @kestrel.retry
      @kestrel.get(@queue) # simulate next get run
    end

    it "processes retries that should retry" do
      returns = [Kestrel::Client::Transactional::RetryableJob.new(1, :mcguffin)]
      stub(@raw_kestrel_client).get(@queue + "_errors", anything) { returns.shift }
      stub(@raw_kestrel_client).get(@queue, anything)
      mock(@raw_kestrel_client).set(@queue + "_errors", anything) do |q,j|
        j.retries.should == 2
        j.job.should == :mcguffin
      end
      mock(@raw_kestrel_client).get_from_last(@queue + "_errors/close")

      get_job.should == :mcguffin
      @kestrel.current_try.should == 2

      @kestrel.retry
      @kestrel.get(@queue) # simulate next get run
    end

    it "processes retries that should give up" do
      returns = [Kestrel::Client::Transactional::RetryableJob.new(Kestrel::Client::Transactional::DEFAULT_RETRIES - 1, :mcguffin)]
      stub(@raw_kestrel_client).get(@queue + "_errors", anything) { returns.shift }
      stub(@raw_kestrel_client).get(@queue, anything)
      mock(@raw_kestrel_client).set.never
      mock(@raw_kestrel_client).get_from_last(@queue + "_errors/close")

      get_job.should == :mcguffin
      @kestrel.current_try.should == Kestrel::Client::Transactional::DEFAULT_RETRIES

      @kestrel.retry
      @kestrel.get(@queue) # simulate next get run
    end
  end

  describe "Instance Methods" do
    before do
      stub(@kestrel).rand { 1 }
    end

    describe "#get" do
      it "asks for a transaction" do
        mock(@raw_kestrel_client).get(@queue, :open => true) { :mcguffin }
        @kestrel.get(@queue).should == :mcguffin
      end

      it "is nil when the primary queue is empty and selected" do
        mock(@kestrel).rand { Kestrel::Client::Transactional::ERROR_PROCESSING_RATE + 0.05 }
        mock(@raw_kestrel_client).get(@queue, anything) { nil }
        mock(@raw_kestrel_client).get(@queue + "_errors", anything).never
        @kestrel.get(@queue).should be_nil
      end

      it "is nil when the error queue is empty and selected" do
        mock(@kestrel).rand { Kestrel::Client::Transactional::ERROR_PROCESSING_RATE - 0.05 }
        mock(@raw_kestrel_client).get(@queue, anything).never
        mock(@raw_kestrel_client).get(@queue + "_errors", anything) { nil }
        @kestrel.get(@queue).should be_nil
      end

      it "returns the payload of a RetryableJob" do
        stub(@kestrel).rand { 0 }
        mock(@raw_kestrel_client).get(@queue + "_errors", anything) do
          Kestrel::Client::Transactional::RetryableJob.new(1, :mcmuffin)
        end

        @kestrel.get(@queue).should == :mcmuffin
      end

      it "closes an open transaction with no retries" do
        stub(@raw_kestrel_client).get(@queue, anything) { :mcguffin }
        @kestrel.get(@queue)

        mock(@raw_kestrel_client).get_from_last(@queue + "/close")
        @kestrel.get(@queue)
      end

      it "closes an open transaction with retries" do
        stub(@kestrel).rand { 0 }
        stub(@raw_kestrel_client).get(@queue + "_errors", anything) do
          Kestrel::Client::Transactional::RetryableJob.new(1, :mcmuffin)
        end
        @kestrel.get(@queue)

        mock(@raw_kestrel_client).get_from_last(@queue + "_errors/close")
        @kestrel.get(@queue)
      end

      it "prevents transactional gets across multiple queues" do
        stub(@raw_kestrel_client).get(@queue, anything) { :mcguffin }
        @kestrel.get(@queue)

        lambda do
          @kestrel.get("transaction_fail")
        end.should raise_error(Kestrel::Client::Transactional::MultipleQueueException)
      end
    end

    describe "#retry" do
      before do
        stub(@raw_kestrel_client).get(@queue, anything) { :mcmuffin }
        stub(@raw_kestrel_client).get_from_last
        @kestrel.get(@queue)
      end

      it "enqueues a fresh failed job to the errors queue with a retry count" do
        mock(@raw_kestrel_client).set(@queue + "_errors", anything) do |queue, job|
          job.retries.should == 1
          job.job.should == :mcmuffin
        end
        @kestrel.retry.should be_true
      end

      it "allows specification of the job to retry" do
        mock(@raw_kestrel_client).set(@queue + "_errors", anything) do |queue, job|
          job.retries.should == 1
          job.job.should == :revised_mcmuffin
        end
        @kestrel.retry(:revised_mcmuffin).should be_true
      end

      it "increments the retry count and re-enqueues the retried job" do
        stub(@kestrel).rand { 0 }
        stub(@raw_kestrel_client).get(@queue + "_errors", anything) do
          Kestrel::Client::Transactional::RetryableJob.new(1, :mcmuffin)
        end

        mock(@raw_kestrel_client).set(@queue + "_errors", anything) do |queue, job|
          job.retries.should == 2
          job.job.should == :mcmuffin
        end

        @kestrel.get(@queue)
        @kestrel.retry.should be_true
      end

      it "does not enqueue the retried job after too many tries" do
        stub(@kestrel).rand { 0 }
        stub(@raw_kestrel_client).get(@queue + "_errors", anything) do
          Kestrel::Client::Transactional::RetryableJob.new(Kestrel::Client::Transactional::DEFAULT_RETRIES - 1, :mcmuffin)
        end
        mock(@raw_kestrel_client).set(@queue + "_errors", anything).never
        mock(@raw_kestrel_client).get_from_last(@queue + "_errors/close")
        @kestrel.get(@queue)
        @kestrel.retry.should be_false
      end

      it "closes an open transaction with no retries" do
        stub(@raw_kestrel_client).get(@queue, anything) { :mcguffin }
        @kestrel.get(@queue)

        mock(@raw_kestrel_client).get_from_last(@queue + "/close")
        @kestrel.retry
      end

      it "closes an open transaction with retries" do
        stub(@kestrel).rand { 0 }
        stub(@raw_kestrel_client).get(@queue + "_errors", anything) do
          Kestrel::Client::Transactional::RetryableJob.new(1, :mcmuffin)
        end
        @kestrel.get(@queue)

        mock(@raw_kestrel_client).get_from_last(@queue + "_errors/close")
        @kestrel.retry
      end
    end

    describe "#read_from_error_queue?" do
      it "returns the error queue ERROR_PROCESSING_RATE pct. of the time" do
        mock(@kestrel).rand { Kestrel::Client::Transactional::ERROR_PROCESSING_RATE - 0.05 }
        @kestrel.send(:read_from_error_queue?).should == true
      end

      it "returns the normal queue most of the time" do
        mock(@kestrel).rand { Kestrel::Client::Transactional::ERROR_PROCESSING_RATE + 0.05 }
        @kestrel.send(:read_from_error_queue?).should == false
      end
    end

    describe "#close_last_transaction" do
      it "does nothing if there is no last transaction" do
        mock(@raw_kestrel_client).get_from_last(@queue + "/close").never
        mock(@raw_kestrel_client).get_from_last(@queue + "_errors/close").never
        @kestrel.send(:close_last_transaction)
      end

      it "closes the normal queue if the job was pulled off of the normal queue" do
        mock(@kestrel).read_from_error_queue? { false }
        mock(@raw_kestrel_client).get(@queue, :open => true) { :mcguffin }
        mock(@raw_kestrel_client).get_from_last(@queue + "/close")
        mock(@raw_kestrel_client).get_from_last(@queue + "_errors/close").never

        @kestrel.get(@queue).should == :mcguffin
        @kestrel.send(:close_last_transaction)
      end

      it "closes the error queue if the job was pulled off of the error queue" do
        mock(@kestrel).read_from_error_queue? { true }
        mock(@raw_kestrel_client).get(@queue + "_errors", anything) { Kestrel::Client::Transactional::RetryableJob.new 1, :mcguffin }
        mock(@raw_kestrel_client).get_from_last(@queue + "/close").never
        mock(@raw_kestrel_client).get_from_last(@queue + "_errors/close")

        @kestrel.get(@queue).should == :mcguffin
        @kestrel.send(:close_last_transaction)
      end
    end
  end
end
