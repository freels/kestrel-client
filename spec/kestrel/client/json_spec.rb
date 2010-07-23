require 'spec/spec_helper'

require 'kestrel/client/json'

describe Kestrel::Client::Json do
  describe "Instance Methods" do
    before do
      @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
      @kestrel = Kestrel::Client::Json.new(@raw_kestrel_client)
    end

    describe "#get" do
      it "parses json" do
        mock(@raw_kestrel_client).get('a_queue') { '{"a": 1, "b": [{"c": 2}]}' }
        @kestrel.get('a_queue').should == {"a" => 1, "b" => ["c" => 2]}
      end

      # wtf is up with this test?
      it "uses a HashWithIndifferentAccess" do
        mock(@raw_kestrel_client).get('a_queue') { '{"a": 1, "b": [{"c": 2}]}' }
        @kestrel.get('a_queue').class.should == HashWithIndifferentAccess
      end

      it "passes through non-strings" do
        mock(@raw_kestrel_client).get('a_queue') { {:key => "value"} }
        @kestrel.get('a_queue').should == {:key => "value"}
      end

      it "passes through strings that are not json" do
        mock(@raw_kestrel_client).get('a_queue') { "I am not JSON" }
        @kestrel.get('a_queue').should == "I am not JSON"
      end
    end
  end
end

class HashWithIndifferentAccess < Hash
  def initialize(hash = {})
    super()
    merge!(hash)
  end
end
