# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "json"

# Read events from the blockchain streaming api.
class LogStash::Inputs::Blockchain < LogStash::Inputs::Base
  config_name "blockchain"
  milestone 1
  default :codec, "json"

  # Websocket URL if not default blockchain. 
  config :wsurl, :validate => :string, :default => "ws://ws.blockchain.info/inv"

  # WS API Operation (see: https://blockchain.info/api/api_websocket)
  config :op, :validate => :string, :default => "unconfirmed_sub"

  #Bitcoin address if addr_sub specified.
  config :bc_addr, :validate => :array


  public
  def register
    require "em-websocket-client"
    if @op == 'addr_sub'
      @wsmsg = '{"op":"'+@op+'"}, "addr":"'+@bc_addr+'"}'
    else
      @wsmsg = '{"op":"'+@op+'"}'
    end
  end

  public 
  def run(output_queue)
    EM.run do

      puts "URL is - #{@wsurl}"
      puts "OP is - #{@op}"
      puts "MSG is - #{@wsmsg}"

      conn = EventMachine::WebSocketClient.connect("#{@wsurl}")

      conn.callback do
        conn.send_msg "#{@wsmsg}"
      end

      conn.errback do |e|
        puts "Got error: #{e}"
      end

      conn.stream do |msg|
        @codec.decode(msg) do |event|
          decorate(event)
          puts event
          output_queue << event
          #puts "<#{msg}>"
          if msg.data == "done"
            conn.close_connection
          end
        end
      end

      conn.disconnect do
        puts "gone"
        EM::stop_event_loop
      end
    end
  end # def run
end # class LogStash::Inputs::Blockchain
