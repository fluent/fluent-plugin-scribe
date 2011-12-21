require 'test/unit'
require 'fluent/test'
require 'lib/fluent/plugin/in_scribe'

class ScribeInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    port 14630
    bind 127.0.0.1
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::ScribeInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 14630, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
  end

  def test_time
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d.expect_emit "tag1", time, {"message"=>'aiueo'}
    d.expect_emit "tag2", time, {"message"=>'aiueo'}

    emits = [
             ['tag1', time, {"message"=>'aiueo'}],
             ['tag2', time, {"message"=>'aiueo'}],
            ]
    d.run do
      emits.each { |tag, time, record|
        res = send(tag, record['message'])
        assert_equal ResultCode::OK, res
      }
    end
  end

  def test_add_prefix
    d = create_driver(CONFIG + %[
      add_prefix scribe
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d.expect_emit "scribe.tag1", time, {"message"=>'aiueo'}
    d.expect_emit "scribe.tag2", time, {"message"=>'aiueo'}

    emits = [
             ['tag1', time, {"message"=>'aiueo'}],
             ['tag2', time, {"message"=>'aiueo'}],
            ]
    d.run do
      emits.each { |tag, time, record|
        res = send(tag, record['message'])
        assert_equal ResultCode::OK, res
      }
    end

    d2 = create_driver(CONFIG + %[
      add_prefix scribe.input
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d2.expect_emit "scribe.input.tag3", time, {"message"=>'aiueo'}
    d2.expect_emit "scribe.input.tag4", time, {"message"=>'aiueo'}

    emits = [
             ['tag3', time, {"message"=>'aiueo'}],
             ['tag4', time, {"message"=>'aiueo'}],
            ]
    d2.run do
      emits.each { |tag, time, record|
        res = send(tag, record['message'])
        assert_equal ResultCode::OK, res
      }
    end
  end

  def send(tag, msg)
    socket = Thrift::Socket.new '127.0.0.1', 14630
    transport = Thrift::FramedTransport.new socket
    protocol = Thrift::BinaryProtocol.new transport, false, false
    client = Scribe::Client.new protocol
    transport.open
    raw_sock = socket.to_io
    raw_sock.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1
    entry = LogEntry.new
    entry.category = tag
    entry.message = msg.to_s
    res = client.Log([entry])
    transport.close
    res
  end
end
