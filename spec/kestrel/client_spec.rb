require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

describe Kestrel::Client do
  describe "Instance Methods" do
    before do
      @kestrel = Kestrel::Client.new(*Kestrel::Config.default)
      stub(@kestrel).with_timing(anything) { |_, block| block.call }
    end

    describe "#get and #set" do
      it "basic operation" do
        @kestrel.flush(queue = "test_queue")
        @kestrel.set(queue, value = "russell's reserve")
        @kestrel.get(queue).should == value
      end
    end

    describe "#flush" do
      before do
        @queue = "some_random_queue_#{Time.now.to_i}_#{rand(10000)}"
      end

      it "counts the number of items flushed and passes each of them to a given block" do
        %w{A B C}.each { |item| @kestrel.set(@queue, item) }
        @kestrel.flush(@queue).should == 3
      end

      it "does not attempt to Marshal load the data being flushed" do
        @kestrel.set(@queue, "some_stuff", 0, true)
        mock(Marshal).load.never
        @kestrel.flush(@queue).should == 1
      end
    end

    describe "#peek" do
      it "should return first item from the queue and reenqueue" do
        @queue = "some_random_queue_#{Time.now.to_i}_#{rand(10000)}"
        @kestrel.set(@queue, "item_1")
        @kestrel.set(@queue, "item_2")
        @kestrel.peek(@queue).should == "item_1"
        @kestrel.sizeof(@queue).should == 2
      end
    end

    describe "#stats" do
      it "retrieves stats" do
        @kestrel.set("test-queue-name", 97)

        stats = @kestrel.stats
        %w{uptime time version curr_items total_items bytes curr_connections total_connections
           cmd_get cmd_set get_hits get_misses bytes_read bytes_written queues}.each do |stat|
          stats[stat].should_not be_nil
        end

        stats['queues']["test-queue-name"].should_not be_nil
        Kestrel::Client::QUEUE_STAT_NAMES.each do |queue_stat|
          stats['queues']['test-queue-name'][queue_stat].should_not be_nil
        end
      end

      it "merge in stats from all the servers" do
        server = @kestrel.servers.first
        stub(@kestrel).servers { [server] }
        stats_for_one_server = @kestrel.stats

        server = @kestrel.servers.first
        stub(@kestrel).servers { [server] * 2 }
        stats_for_two_servers = @kestrel.stats

        stats_for_two_servers['bytes'].should == 2*stats_for_one_server['bytes']
      end
    end

    describe "#stat" do
      it "get stats for single queue" do
        @kestrel.set(queue = "test-queue-name", 97)
        all_stats = @kestrel.stats
        @kestrel.stat(queue).should == all_stats['queues'][queue]
      end
    end

    describe "#sizeof" do
      before do
        @queue = "some_random_queue_#{Time.now.to_i}_#{rand(10000)}"
      end

      it "reports the size of the queue" do
        100.times { @kestrel.set(@queue, true) }
        @kestrel.sizeof(@queue).should == 100
      end

      it "reports the size of a non-existant queue as 0" do
        queue = "some_random_queue_#{Time.now.to_i}_#{rand(10000)}"
        @kestrel.sizeof(queue).should == 0
      end
    end

    describe "#available_queues" do
      it "returns all the queue names" do
        @kestrel.set("test-queue-name1", 97)
        @kestrel.set("test-queue-name2", 155)
        @kestrel.available_queues.should include('test-queue-name1')
        @kestrel.available_queues.should include('test-queue-name2')
      end
    end
  end
end
