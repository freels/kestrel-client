require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

class Envelope; end

describe Kestrel::Client::Envelope do
  describe "Instance Methods" do
    before do
      @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
      @kestrel = Kestrel::Client::Envelope.new(Envelope, @raw_kestrel_client)
    end

    describe "#get and #set" do
      describe "envelopes" do
        it "creates an envelope on a set" do
          mock(Envelope).new(:mcguffin)
          @kestrel.set('a_queue', :mcguffin)
        end

        it "unwraps an envelope on a get" do
          envelope = Envelope.new
          mock(@raw_kestrel_client).get('a_queue') { envelope }
          mock(envelope).unwrap { :mcguffin }
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
