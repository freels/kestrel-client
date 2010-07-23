require 'spec/spec_helper'

describe Kestrel::Client::Retrying do
  before do
    @raw_kestrel_client = Kestrel::Client.new(*Kestrel::Config.default)
    @kestrel = Kestrel::Client::Retrying.new(@raw_kestrel_client)
    @queue = "some_queue"
  end

  it "does not retry if no exception is raised" do
    mock(@raw_kestrel_client).get(@queue) { :mcguffin }
    lambda do
      @kestrel.get(@queue).should == :mcguffin
    end.should_not raise_error
  end

  ['get', 'set', 'delete'].each do |operation|
    it "retries DEFAULT_RETRY_COUNT times then fails" do
      mock(@raw_kestrel_client).send(operation, @queue) { raise Memcached::ServerIsMarkedDead }.
        times(Kestrel::Client::Retrying::DEFAULT_RETRY_COUNT + 1)

      lambda do
        @kestrel.send(operation, @queue)
      end.should raise_error(Memcached::ServerIsMarkedDead)
    end

    it "does not retry on non-connection related exceptions" do
      [Memcached::ABadKeyWasProvidedOrCharactersOutOfRange,
       Memcached::ActionQueued,
       Memcached::NoServersDefined].each do |ex|

        mock(@raw_kestrel_client).send(operation, @queue) { raise ex }
        lambda { @kestrel.send(operation, @queue) }.should raise_error(ex)

      end
    end

    it "does not retry when retry count is 0" do
      kestrel = Kestrel::Client::Retrying.new(@raw_kestrel_client, 0)
      mock(@raw_kestrel_client).send(operation, @queue) { raise Memcached::ServerIsMarkedDead }
      lambda { kestrel.send(operation, @queue) }.should raise_error(Memcached::ServerIsMarkedDead)
    end

  end

end
