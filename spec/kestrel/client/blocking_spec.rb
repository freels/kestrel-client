require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

describe "Kestrel::Client::Blocking" do
  describe "Instance Methods" do
    before do
      Kestrel::Config.load TEST_CONFIG_FILE
      @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
      @kestrel = Kestrel::Client::Blocking.new(@raw_kestrel_client)
    end

    describe "#set" do
      before do
        @queue = "some_queue"
        @value = "some_value"
      end

      it "blocks on a set until the set works" do
        @queue = "some_queue"
        @value = "some_value"
        mock(@raw_kestrel_client)\
          .set(@queue, @value, anything, anything) { raise Memcached::Error }.then\
          .set(@queue, @value, anything, anything) { :mcguffin }
        mock(@kestrel).sleep(Kestrel::Client::Blocking::WAIT_TIME_BEFORE_RETRY).once
        @kestrel.set(@queue, @value).should == :mcguffin
      end

      it "raises if two sets in a row fail" do
        mock(@raw_kestrel_client)\
          .set(@queue, @value, anything, anything) { raise Memcached::Error }.then\
          .set(@queue, @value, anything, anything) { raise Memcached::Error }
        mock(@kestrel).sleep(Kestrel::Client::Blocking::WAIT_TIME_BEFORE_RETRY).once
        lambda { @kestrel.set(@queue, @value) }.should raise_error(Memcached::Error)
      end

      it "passes along the default expiry time if none is given" do
        @queue = "some_queue"
        @value = "some_value"
        mock(@raw_kestrel_client).set(@queue, @value, Kestrel::Client::Blocking::DEFAULT_EXPIRY, anything)
        @kestrel.set(@queue, @value)
      end

      it "passes along the given expiry time if one is passed in" do
        @queue = "some_queue"
        @value = "some_value"
        mock(@raw_kestrel_client).set(@queue, @value, 60, anything)
        @kestrel.set(@queue, @value, 60)
      end
    end

    describe "#get" do
      before do
        @queue = "some_queue"
      end

      it "blocks on a get until the get works" do
        mock(@raw_kestrel_client)\
          .get(@queue) { nil }.then\
          .get(@queue) { :mcguffin }
        mock(@kestrel).sleep(Kestrel::Client::Blocking::WAIT_TIME_BEFORE_RETRY).once
        @kestrel.get(@queue).should == :mcguffin
      end

      describe "#get_without_blocking" do
        it "does not block" do
          mock(@raw_kestrel_client).get(@queue) { nil }
          mock(@kestrel).sleep(Kestrel::Client::Blocking::WAIT_TIME_BEFORE_RETRY).never
          @kestrel.get_without_blocking(@queue).should be_nil
        end
      end
    end
  end
end
