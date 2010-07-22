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
