require 'spec/spec_helper'

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
          get(@queue) { nil }.times(5).then.get(@queue) { :mcguffin }
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
