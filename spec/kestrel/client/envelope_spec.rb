require 'spec/spec_helper'

class Envelope
  class << self; attr_accessor :unwraps end

  def initialize(item); @item = item end
  def unwrap; self.class.unwraps += 1; @item end
end

describe Kestrel::Client::Envelope do
  describe "Instance Methods" do
    before do
      Envelope.unwraps = 0
      @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
      @kestrel = Kestrel::Client::Envelope.new(Envelope, @raw_kestrel_client)
    end

    describe "#get and #set" do
      describe "envelopes" do
        it "integrates" do
          @kestrel.set("a_queue", :mcguffin)
          @kestrel.get("a_queue").should == :mcguffin
          Envelope.unwraps.should == 1
        end

        it "creates an envelope on a set" do
          mock(Envelope).new(:mcguffin)
          @kestrel.set('a_queue', :mcguffin)
        end

        it "unwraps an envelope on a get" do
          envelope = Envelope.new(:mcguffin)
          mock(@raw_kestrel_client).get('a_queue') { envelope }
          mock.proxy(envelope).unwrap
          @kestrel.get('a_queue').should == :mcguffin
        end

        it "does not unwrap a nil get" do
          mock(@raw_kestrel_client).get('a_queue') { nil }
          @kestrel.get('a_queue').should be_nil
        end
      end
    end
  end
end
