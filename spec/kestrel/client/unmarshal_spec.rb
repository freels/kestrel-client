require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

describe Kestrel::Client::Unmarshal do
  describe "Instance Methods" do
    before do
      @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
      @kestrel = Kestrel::Client::Unmarshal.new(@raw_kestrel_client)
    end

    describe "#get" do
      it "unmarshals marshaled objects" do
        test_object = {:a => 1, :b => [1, 2, 3]}
        mock(@raw_kestrel_client).get('a_queue', true) { Marshal.dump(test_object) }
        @kestrel.get('a_queue').should == test_object
      end

      it "does not unmarshal when raw is true" do
        test_object = {:a => 1, :b => [1, 2, 3]}
        mock(@raw_kestrel_client).get('a_queue', true) { Marshal.dump(test_object) }
        @kestrel.get('a_queue', true).should == Marshal.dump(test_object)
      end

      it "pasess through objects" do
        test_object = Object.new
        mock(@raw_kestrel_client).get('a_queue', true) { test_object }
        @kestrel.get('a_queue').should == test_object
      end

      it "passes through strings" do
        mock(@raw_kestrel_client).get('a_queue', true) { "I am not marshaled" }
        @kestrel.get('a_queue').should == "I am not marshaled"
      end
    end

    describe "#isMarshaled" do
      it "should foo" do
        @kestrel.is_marshaled?("foo").should be_false
        @kestrel.is_marshaled?(Marshal.dump("foo")).should be_true

        @kestrel.is_marshaled?({}).should be_false
        @kestrel.is_marshaled?(Marshal.dump({})).should be_true

        @kestrel.is_marshaled?(BadObject.new).should be_false
        @kestrel.is_marshaled?(Marshal.dump(BadObject.new)).should be_true
      end
    end
  end
end

class BadObject
  def to_s
    raise Exception
  end
end

module Marshal
  def self.load_with_constantize(source, loaded_constants = [])
    self.load(source)
  end
end
