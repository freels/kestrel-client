require 'spec/spec_helper'

describe Kestrel::Client::Partitioning do
  before do
    @client_1 = Kestrel::Client.new(*Kestrel::Config.default)
    @client_2 = Kestrel::Client.new(*Kestrel::Config.default)
    @default_client = Kestrel::Client.new(*Kestrel::Config.default)

    @kestrel = Kestrel::Client::Partitioning.new('queue1' => @client_1, ['queue2', 'queue3'] => @client_2, :default => @default_client)
  end

  %w(set get delete flush stat).each do |method|
    describe "##{method}" do
      it "routes to the correct client" do
        mock(@client_1).__send__(method, 'queue1')
        @kestrel.send(method, 'queue1')

        mock(@client_2).__send__(method, 'queue2')
        @kestrel.send(method, 'queue2')

        mock(@client_2).__send__(method, 'queue3/queue_arg')
        @kestrel.send(method, 'queue3/queue_arg')

        mock(@default_client).__send__(method, 'queue4')
        @kestrel.send(method, 'queue4')
      end
    end
  end
end
