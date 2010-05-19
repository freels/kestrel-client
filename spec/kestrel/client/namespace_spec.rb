require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

describe Kestrel::Client::Namespace do
  describe "Instance Methods" do
    before do
      @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
      @kestrel = Kestrel::Client::Namespace.new('some_namespace', @raw_kestrel_client)
    end

    describe "#set" do
      it "prepends a namespace to the key" do
        mock(@raw_kestrel_client).set('some_namespace:a_queue', :mcguffin)
        @kestrel.set('a_queue', :mcguffin)
      end
    end

    describe "#get" do
      it "prepends a namespace to the key" do
        mock(@raw_kestrel_client).get('some_namespace:a_queue')
        @kestrel.get('a_queue')
      end
    end

    describe "#delete" do
      it "prepends a namespace to the key" do
        mock(@raw_kestrel_client).delete('some_namespace:a_queue')
        @kestrel.delete('a_queue')
      end
    end

    describe "#flush" do
      it "prepends a namespace to the key" do
        mock(@raw_kestrel_client).flush('some_namespace:a_queue')
        @kestrel.flush('a_queue')
      end
    end

    describe "#stat" do
      it "prepends a namespace to the key" do
        mock(@raw_kestrel_client).stat('some_namespace:a_queue')
        @kestrel.stat('a_queue')
      end
    end

    describe "#available_queues" do
      it "only returns namespaced queues" do
        @raw_kestrel_client.set('some_namespace:namespaced_queue', 'foo')
        @raw_kestrel_client.set('unnamespaced_queue', 'foo')

        @kestrel.available_queues.should == ['namespaced_queue']
      end
    end
  end
end
