module Kestrel::Client::RetryHelper

  private

  def with_retries #:nodoc:
    yield
  rescue *Kestrel::Client::RECOVERABLE_ERRORS => e
    unless e.instance_of?(Memcached::SystemError) && e.message =~ /Operation now in progress/
      tries ||= @exception_retry_limit + 1
      tries -= 1
      tries > 0 ? retry : raise
    end
  end
end
