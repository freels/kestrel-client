require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

describe "Kestrel::Client::Reliable" do
  describe "Instance Methods" do
    before do
      @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
      @kestrel = Kestrel::Client::Reliable.new(@raw_kestrel_client)
    end

    describe "#get" do
      before do
        @queue = "some_queue"
        stub(@kestrel).rand { 1 }
      end

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

    end

  end
end
