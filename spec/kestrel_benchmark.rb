require 'spec/spec_helper'
require 'benchmark'

describe Kestrel::Client do
  before do
    @queue = "a_queue"
    @kestrel = Kestrel::Client.new(*Kestrel::Config.default)

    @kestrel.delete(@queue) rescue nil # Memcache::ServerEnd bug
  end

  it "is fast" do
    @kestrel.flush(@queue)
    @value = { :value => "a value" }
    @raw_value = Marshal.dump(@value)

    times = 10_000

    Benchmark.bm do |x|
      x.report("set:") { for i in 1..times; @kestrel.set(@queue, @value); end }
      x.report("get:") { for i in 1..times; @kestrel.get(@queue); end }
      x.report("set (raw):") { for i in 1..times; @kestrel.set(@queue, @raw_value, 0, true); end }
      x.report("get (raw):") { for i in 1..times; @kestrel.get(@queue, true); end }
    end
  end
end
