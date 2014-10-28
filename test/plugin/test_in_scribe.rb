require 'test/unit'
require 'fluent/test'
require 'fluent/plugin/in_scribe'

require 'thrift'

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

  def shutdown_driver(driver)
    return unless driver.instance.instance_eval{ @thread }
    driver.instance.shutdown
    driver.instance.instance_eval{ @thread && @thread.join }
  end

  def test_configure
    d = create_driver
    assert_equal 14630, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
    assert_equal false, d.instance.remove_newline
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
        res = message_send(tag, record['message'])
        assert_equal ResultCode::OK, res
      }
    end

    shutdown_driver(d)
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
        res = message_send(tag, record['message'])
        assert_equal ResultCode::OK, res
      }
    end

    shutdown_driver(d)

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
        res = message_send(tag, record['message'])
        assert_equal ResultCode::OK, res
      }
    end

    shutdown_driver(d2)
  end

  def test_remove_newline
    d = create_driver(CONFIG + %[
      remove_newline true
    ])
    assert_equal true, d.instance.remove_newline

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d.expect_emit "tag1", time, {"message"=>'aiueo'}
    d.expect_emit "tag2", time, {"message"=>'kakikukeko'}
    d.expect_emit "tag3", time, {"message"=>'sasisuseso'}

    emits = [
             ['tag1', time, {"message"=>"aiueo\n"}],
             ['tag2', time, {"message"=>"kakikukeko\n"}],
             ['tag3', time, {"message"=>"sasisuseso"}],
            ]
    d.run do
      emits.each { |tag, time, record|
        res = message_send(tag, record['message'])
        assert_equal ResultCode::OK, res
      }
    end

    shutdown_driver(d)
  end

  def test_msg_format_json
    d = create_driver(CONFIG + %[
      msg_format json
    ])
    assert_equal :json, d.instance.msg_format

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>1, "b"=>2}
    d.expect_emit "tag3", time, {"a"=>1, "b"=>2, "c"=>3}

    emits = [
             ['tag1', time, {"a"=>1}.to_json],
             ['tag2', time, {"a"=>1, "b"=>2}.to_json],
             ['tag3', time, {"a"=>1, "b"=>2, "c"=>3}.to_json],
            ]
    d.run do
      emits.each { |tag, time, message|
        res = message_send(tag, message)
        assert_equal ResultCode::OK, res
      }
    end

    shutdown_driver(d)
  end

  data do
    Fluent::ScribeInput.new
    {
      'true'  => [ResultCode::OK, true],
      'false' => [ResultCode::TRY_LATER, false]
    }
  end
  def test_msg_format_json_with_ignore_invalid_record(data)
    result, opt = data
    d = create_driver(CONFIG + %[
      msg_format json
      ignore_invalid_record #{opt}
    ])
    assert_equal :json, d.instance.msg_format
    assert_equal opt, d.instance.ignore_invalid_record

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    emits = [['tag1', time, '{"a":']]
    d.run do
      emits.each { |tag, time, message|
        res = message_send(tag, message)
        assert_equal result, res
      }
    end

    shutdown_driver(d)
  end

  def test_msg_format_url_param
    d = create_driver(CONFIG + %[
      msg_format url_param
    ])
    assert_equal :url_param, d.instance.msg_format

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d.expect_emit "tag0", time, {}
    d.expect_emit "tag1", time, {"a"=>'1'}
    d.expect_emit "tag2", time, {"a"=>'1', "b"=>'2'}
    d.expect_emit "tag3", time, {"a"=>'1', "b"=>'2', "c"=>'3'}
    d.expect_emit "tag4", time, {"a"=>'1', "b"=>'2', "c"=>'3=4'}

    emits = [
             ['tag0', time, ""],
             ['tag1', time, "a=1"],
             ['tag2', time, "a=1&b=2"],
             ['tag3', time, "a=1&b=2&c=3"],
             ['tag4', time, "a=1&b=2&c=3=4"],
            ]
    d.run do
      emits.each { |tag, time, message|
        res = message_send(tag, message)
        assert_equal ResultCode::OK, res
      }
    end

    shutdown_driver(d)
  end

  def message_send(tag, msg)
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
