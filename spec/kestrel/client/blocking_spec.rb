require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

describe "Kestrel::Client::Blocking" do
  describe "Instance Methods" do
    before do
      @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
      @kestrel = Kestrel::Client::Blocking.new(@raw_kestrel_client)
    end

    describe "#get" do
      before do
        @queue = "some_queue"
      end

      it "blocks on a get until the get works" do
        mock(@raw_kestrel_client).
          get(@queue, :raw => false, :timeout => Kestrel::Client::Blocking::DEFAULT_TIMEOUT) { nil }.then.
          get(@queue, :raw => false, :timeout => Kestrel::Client::Blocking::DEFAULT_TIMEOUT) { :mcguffin }
        @kestrel.get(@queue).should == :mcguffin
      end

      describe "#get_without_blocking" do
        it "does not block" do
          mock(@raw_kestrel_client).get(@queue) { nil }
          @kestrel.get_without_blocking(@queue).should be_nil
        end
      end
    end
  end
end
