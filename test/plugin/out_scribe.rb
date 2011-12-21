require 'test/unit'
require 'fluent/test'
require 'lib/fluent/plugin/out_scribe'

class ScribeOutputTest < Test::Unit::TestCase
  CONFIG = %[
    host 127.0.0.1
    port 14630
  ]

  def create_driver(conf=CONFIG, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::ScribeOutput, tag).configure(conf)
  end

  def test_configure
    d = create_driver('')

    assert_equal 'localhost', d.instance.host
    assert_equal 1463, d.instance.port
    assert_equal 'message', d.instance.field_ref
    assert_equal 30, d.instance.timeout
    assert_equal 'unknown', d.instance.default_category
    assert_nil d.instance.remove_prefix

    d = create_driver

    assert_equal '127.0.0.1', d.instance.host
    assert_equal 14630, d.instance.port
  end

  def test_format
    time = Time.parse("2011-12-21 13:14:15 UTC").to_i

    d = create_driver
    d.emit({"message" => "testing first", "message2" => "testing first another data"}, time)
    d.emit({"message" => "testing second", "message2" => "testing second another data"}, time)
    d.emit({"message" => "testing third", "message2" => "testing third another data"}, time)
    d.expect_format [d.tag, {"message" => "testing first", "message2" => "testing first another data"}].to_msgpack
    d.expect_format [d.tag, {"message" => "testing second", "message2" => "testing second another data"}].to_msgpack
    d.expect_format [d.tag, {"message" => "testing third", "message2" => "testing third another data"}].to_msgpack
    d.run

    d = create_driver(CONFIG + %[
field_ref message2
remove_prefix test
    ], 'test.scribeplugin')
    assert_equal 'test.scribeplugin', d.tag

    d.emit({"message" => "xxx testing first", "message2" => "xxx testing first another data"}, time)
    d.emit({"message" => "xxx testing second", "message2" => "xxx testing second another data"}, time)
    d.expect_format ['scribeplugin', {"message" => "xxx testing first", "message2" => "xxx testing first another data"}].to_msgpack
    d.expect_format ['scribeplugin', {"message" => "xxx testing second", "message2" => "xxx testing second another data"}].to_msgpack
    d.run

    d = create_driver(CONFIG + %[
field_ref message2
remove_prefix test
    ], 'xxx.test.scribeplugin')
    assert_equal 'xxx.test.scribeplugin', d.tag
    d.emit({"message" => "xxx testing first", "message2" => "xxx testing first another data"}, time)
    d.expect_format ['xxx.test.scribeplugin', {"message" => "xxx testing first", "message2" => "xxx testing first another data"}].to_msgpack
    d.run

    d = create_driver(CONFIG + %[
field_ref message2
remove_prefix test
    ], 'test')
    assert_equal 'test', d.tag
    d.emit({"message" => "xxx testing first", "message2" => "xxx testing first another data"}, time)
    d.expect_format ['unknown', {"message" => "xxx testing first", "message2" => "xxx testing first another data"}].to_msgpack
    d.run
  end

  def test_write
    time = Time.parse("2011-12-21 13:14:15 UTC").to_i

    d = create_driver
    d.emit({"message" => "testing first", "message2" => "testing first another data"}, time)
    d.emit({"message" => "testing second", "message2" => "testing second another data"}, time)
    d.emit({"message" => "testing third", "message2" => "testing third another data"}, time)
    result = d.run
    assert_equal ResultCode::OK, result
    assert_equal [[d.tag, 'testing first'], [d.tag, 'testing second'], [d.tag,'testing third']], $handler.last

    d = create_driver(CONFIG + %[
field_ref message2
remove_prefix test
    ], 'test.scribeplugin')
    assert_equal 'test.scribeplugin', d.tag
    d.emit({"message" => "xxx testing first", "message2" => "xxx testing first another data"}, time)
    d.emit({"message" => "xxx testing second", "message2" => "xxx testing second another data"}, time)
    result = d.run
    assert_equal ResultCode::OK, result
    assert_equal [['scribeplugin', 'xxx testing first another data'], ['scribeplugin', 'xxx testing second another data']], $handler.last

    d = create_driver(CONFIG + %[
field_ref message2
remove_prefix test
    ], 'xxx.test.scribeplugin')
    assert_equal 'xxx.test.scribeplugin', d.tag
    d.emit({"message" => "yyy testing first", "message2" => "yyy testing first another data"}, time)
    result = d.run
    assert_equal ResultCode::OK, result
    assert_equal [['xxx.test.scribeplugin', 'yyy testing first another data']], $handler.last

    d = create_driver(CONFIG + %[
field_ref message2
remove_prefix test
    ], 'test')
    assert_equal 'test', d.tag
    d.emit({"message" => "zzz testing first", "message2" => "zzz testing first another data"}, time)
    result = d.run
    assert_equal ResultCode::OK, result
    assert_equal [[d.instance.default_category, 'zzz testing first another data']], $handler.last
  end

  def setup
    Fluent::Test.setup
    $handler = TestScribeServerHandler.new
    @dummy_server_thread = Thread.new do
      begin
        transport = Thrift::ServerSocket.new '127.0.0.1', 14630
        processor = Scribe::Processor.new $handler
        transport_factory = Thrift::FramedTransportFactory.new
        protocol_factory = Thrift::BinaryProtocolFactory.new
        protocol_factory.instance_eval {|obj|
          def get_protocol(trans) # override
            Thrift::BinaryProtocol.new(trans, strict_read=false, strict_write=false)
          end
        }
        server = Thrift::SimpleServer.new processor, transport, transport_factory, protocol_factory
        server.serve
      ensure
        transport.close unless transport.closed?
      end
    end
    sleep 0.1 # boo...
  end

  def teardown
    @dummy_server_thread.kill
    @dummy_server_thread.join
  end

  class TestScribeServerHandler
    attr :last
    def initialize
      @last = []
    end
    def Log(msgs)
      @last = msgs.map{|msg| [msg.category, msg.message.force_encoding('UTF-8')]}
      ResultCode::OK
    end
  end
end
