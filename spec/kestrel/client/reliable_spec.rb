require 'spec/spec_helper'

describe "Kestrel::Client::Reliable" do
  describe "Instance Methods" do
    before do
      @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
      @kestrel = Kestrel::Client::Reliable.new(@raw_kestrel_client, nil, nil, 1)
      stub(@kestrel).rand { 1 }
      @queue = "some_queue"
    end

    describe "#get" do

      it "asks for a transaction" do
        mock(@raw_kestrel_client).get(@queue, :raw => false, :open => true, :close => true) { :mcguffin }
        @kestrel.get(@queue).should == :mcguffin
      end

      it "gets from the error queue ERROR_PROCESSING_RATE pct. of the time" do
        mock(@kestrel).rand { Kestrel::Client::Reliable::ERROR_PROCESSING_RATE - 0.05 }
        mock(@raw_kestrel_client).get(@queue + "_errors", anything) { :mcguffin }
        mock(@raw_kestrel_client).get(@queue, anything).never
        @kestrel.get(@queue).should == :mcguffin
      end

      it "falls through to the normal queue when error queue is empty" do
        mock(@kestrel).rand { Kestrel::Client::Reliable::ERROR_PROCESSING_RATE - 0.05 }
        mock(@raw_kestrel_client).get(@queue + "_errors", anything) { nil }
        mock(@raw_kestrel_client).get(@queue, anything) { :mcguffin }
        @kestrel.get(@queue).should == :mcguffin
      end

      it "gets from the normal queue most of the time" do
        mock(@kestrel).rand { Kestrel::Client::Reliable::ERROR_PROCESSING_RATE + 0.05 }
        mock(@raw_kestrel_client).get(@queue, anything) { :mcguffin }
        mock(@raw_kestrel_client).get(@queue + "_errors", anything).never
        @kestrel.get(@queue).should == :mcguffin
      end

      it "falls through to the error queue when normal queue is empty" do
        mock(@kestrel).rand { Kestrel::Client::Reliable::ERROR_PROCESSING_RATE + 0.05 }
        mock(@raw_kestrel_client).get(@queue, anything) { nil }
        mock(@raw_kestrel_client).get(@queue + "_errors", anything) { :mcguffin }
        @kestrel.get(@queue).should == :mcguffin
      end

      it "is nil when both queues are empty" do
        mock(@kestrel).rand { Kestrel::Client::Reliable::ERROR_PROCESSING_RATE + 0.05 }
        mock(@raw_kestrel_client).get(@queue, anything) { nil }
        mock(@raw_kestrel_client).get(@queue + "_errors", anything) { nil }
        @kestrel.get(@queue).should be_nil
      end

      it "returns the payload of a RetryableJob" do
        stub(@kestrel).rand { 0 }
        mock(@raw_kestrel_client).get(@queue + "_errors", anything) do
          Kestrel::Client::Reliable::RetryableJob.new(1, :mcmuffin)
        end

        @kestrel.get(@queue).should == :mcmuffin
        @kestrel.current_try.should == 2
      end

      it "closes an open transaction with no retries" do
        stub(@raw_kestrel_client).get(@queue, anything) { :mcguffin }
        @kestrel.get(@queue)

        mock(@raw_kestrel_client).get_from_last(@queue, :close => true, :open => false)
        @kestrel.get(@queue)
      end

      it "closes an open transaction with retries" do
        stub(@kestrel).rand { 0 }
        stub(@raw_kestrel_client).get(@queue + "_errors", anything) do
          Kestrel::Client::Reliable::RetryableJob.new(1, :mcmuffin)
        end
        @kestrel.get(@queue)

        mock(@raw_kestrel_client).get_from_last(@queue + "_errors", :close => true, :open => false)
        @kestrel.get(@queue)
      end
    end

    describe "#current_try" do

      it "returns 1 if nothing has been gotten" do
        @kestrel.current_try.should == 1
      end

      it "returns 1 for jobs that have not been retried" do
        mock(@raw_kestrel_client).get(@queue, anything) { :mcguffin }
        @kestrel.get(@queue)
        @kestrel.current_try.should == 1
      end

      it "returns 1 plus the number of tries for a RetryableJob" do
        stub(@kestrel).rand { 0 }
        mock(@raw_kestrel_client).get(@queue + "_errors", anything) do
          Kestrel::Client::Reliable::RetryableJob.new(1, :mcmuffin)
        end
        @kestrel.get(@queue)
        @kestrel.current_try.should == 2
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

      it "increments the retry count and re-enqueues the retried job" do
        stub(@kestrel).rand { 0 }
        stub(@raw_kestrel_client).get(@queue + "_errors", anything) do
          Kestrel::Client::Reliable::RetryableJob.new(1, :mcmuffin)
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
          Kestrel::Client::Reliable::RetryableJob.new(Kestrel::Client::Reliable::DEFAULT_RETRIES - 1, :mcmuffin)
        end
        mock(@raw_kestrel_client).set(@queue + "_errors", anything).never
        @kestrel.get(@queue)
        @kestrel.retry.should be_false
      end

      it "closes an open transaction with no retries" do
        stub(@raw_kestrel_client).get(@queue, anything) { :mcguffin }
        @kestrel.get(@queue)

        mock(@raw_kestrel_client).get_from_last(@queue, :close => true, :open => false)
        @kestrel.retry
      end

      it "closes an open transaction with retries" do
        stub(@kestrel).rand { 0 }
        stub(@raw_kestrel_client).get(@queue + "_errors", anything) do
          Kestrel::Client::Reliable::RetryableJob.new(1, :mcmuffin)
        end
        @kestrel.get(@queue)

        mock(@raw_kestrel_client).get_from_last(@queue + "_errors", :close => true, :open => false)
        @kestrel.retry
      end

    end
  end
end
