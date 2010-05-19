require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

describe Kestrel::Client::Namespace do
  describe "Instance Methods" do
    before do
      @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
      @kestrel = Kestrel::Client::Namespace.new('some_namespace', @raw_kestrel_client)
    end

    describe "#get and #set" do
      describe "namespace" do
        it "prepends a namespace to key on a set" do
          mock(@raw_kestrel_client).set('some_namespace:a_queue', :mcguffin)
          @kestrel.set('a_queue', :mcguffin)
        end

        it "prepends a namespace to key on a get" do
          mock(@raw_kestrel_client).get('some_namespace:a_queue')
          @kestrel.get('a_queue')
        end
      end
    end
  end
end
