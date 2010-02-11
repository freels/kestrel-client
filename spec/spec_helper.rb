require 'rubygems'
require 'spec'

spec_dir = File.dirname(__FILE__)

# make sure we load local libs rather than gems first
$: << File.expand_path("#{spec_dir}/../lib")

require 'kestrel'

TEST_CONFIG_FILE = File.expand_path("#{spec_dir}/kestrel/config/kestrel.yml")

Spec::Runner.configure do |config|
  config.mock_with :rr
end
