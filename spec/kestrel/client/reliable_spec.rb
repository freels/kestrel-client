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
      end

      it "asks for a transaction" do
        mock(@raw_kestrel_client).get(@queue + "/close/open") { :mcguffin }
        @kestrel.get(@queue).should == :mcguffin
      end
    end

    describe "#abort" do
      before do
        @queue = "some_queue"
      end

      it "can abort" do
        mock(@raw_kestrel_client).get_from_last(@queue + "/abort") { nil }
        @kestrel.abort(@queue)
      end
    end
  end
end
