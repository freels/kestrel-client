module Kestrel::Client::RetryHelper

  # Exceptions which are connection failures we retry after
  RECOVERABLE_ERRORS = [
    Memcached::ServerIsMarkedDead,
    Memcached::ATimeoutOccurred,
    Memcached::ConnectionBindFailure,
    Memcached::ConnectionFailure,
    Memcached::ConnectionSocketCreateFailure,
    Memcached::Failure,
    Memcached::MemoryAllocationFailure,
    Memcached::ReadFailure,
    Memcached::ServerError,
    Memcached::SystemError,
    Memcached::UnknownReadFailure,
    Memcached::WriteFailure
  ]

  private

  def with_retries #:nodoc:
    yield
  rescue *RECOVERABLE_ERRORS
    tries ||= @exception_retry_limit + 1
    tries -= 1
    tries > 0 ? retry : raise
  end
end
