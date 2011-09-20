#!/usr/bin/env ruby

require 'thread'
require 'zmq'
require 'crack'

Thread.abort_on_exception = true
$ctx = ZMQ::Context.new(10)

class Heartbeat
  attr_accessor :id, :thread, :base, :base_id, :port, :control_port, :attach

	def initialize(args = {})
		@id = args[:id] || Heartbeat.id
		@thread = args[:inproc] || 'me'
		@base = File.expand_path(args[:directory] || '/tmp/management_nodes')
		@base_id = @base + "/#{@id}"
		@port = args[:port] || Heartbeat.port
		@control_port = args[:control_port] || Heartbeat.control_port
		@attach = nil
	end

	def self.id
		r = Random.new
		return r.rand(0..1000)
	end

	def self.port
		r = Random.new
		return r.rand(17330..17340)
	end

	def self.control_port
		r = Random.new
		return r.rand(18330..18340)
	end

	#Heartbeat
	def announce
		@attach = $ctx.socket(ZMQ::PUB)
		@attach.setsockopt(ZMQ::HWM, 100)

		if File.exists?(@base_id) == false
			if File.directory?(@base) == false
				Dir.mkdir(@base)
			end
		else
			@base_id = @base + "/#{@id}"
		end

		counter = 0
		begin
			@attach.bind("ipc://#{@base_id}")
			@attach.bind("inproc://#{@thread}")
			Thread.new { @attach.bind("tcp://*:#{@port}") }
		rescue => error
			puts "#{@id.inspect} HEARTBEAT::ANNOUNCE LOOP::RESCUE: ERROR #{error} - COUNTER: #{counter}"
			@port = Heartbeat.port
			counter = counter + 1
			raise unless counter > 11
			retry
		end

		Thread.new do
			loop do
puts "#{@id.inspect} HEARTBEAT::ANNOUNCE LOOP::SENDING MESSAGES"
				@attach.send('heartbeat', ZMQ::SNDMORE)
puts "#{@id.inspect} HEARTBEAT::ANNOUNCE LOOP::TOPC SENT"
				@attach.send("{ 'id' : #{@id}, 'pid' : #{Process.pid}, 'tcp' : #{@port}, 'file' : #{@base_id}, 'control_port' : #{@control_port} }")
puts "#{@id.inspect} HEARTBEAT::ANNOUNCE LOOP::MESSAGE SENT"
				sleep 1
			end
		end
	end

	#Control Port
	def responder
		Thread.new do
			resp = $ctx.socket(ZMQ::REP)

			counter = 0
			begin
				resp.bind("tcp://*:#{@control_port}")
			rescue => error
				puts "#{@id.inspect} HEARTBEAT::RESPONDER LOOP::RESCUE: ERROR:#{error} - COUNTER:#{counter}"
				@control_port = Heartbeat.control_port
				counter = counter + 1
				raise unless counter > 11
				retry
			end

			loop do
puts "#{@id.inspect} HEARTBEAT::RESPONDER LOOP::WAITING FOR A MESSAGE"
				message = resp.recv
puts "#{@id.inspect} HEARTBEAT::RESPONDER LOOP::MESSAGE RECIEVED: #{message}"

				case message
				when "status?"
					resp.send("running")
puts "#{@id.inspect} HEARTBEAT::RESPONDER LOOP::SENT MESSAGE: RUNNING"
				else
					resp.send("Unknown request!")
puts "#{@id.inspect} HEARTBEAT::RESPONDER LOOP::SENT MESSAGE: Unknown"
				end
			end
		end
	end

	def trap
		Signal.list.each do |k,signal_type|
			Signal.trap(signal_type, "IGNORE")
		end
	end

	def close
		@attach.close
	end
end

class Monitor
  attr_accessor :id, :base, :attach, :results, :management_nodes, :connected_management_nodes

	def initialize(args = {})
		@base = File.expand_path(args[:directory] || '/tmp/management_nodes')
		raise "I have no ID?" unless args[:id] != nil
		@id = args[:id]
		@attach = nil
		@recieve_thread = nil
		@results = Queue.new
		@management_nodes = []
		@connected_management_nodes = []
	end

	#Do I need to duplicate myself?
	def parse_management_nodes
		sleep 25

		if @management_nodes == [] && @connected_management_nodes == []
			b = Management.new
			b.duplicate

			sleep 25
		end
	end

	#Gather All Available Management Nodes
	def gather
		Thread.new do
			loop do
				temp_management_nodes = Dir[@base + '/**'].collect {|b| 'ipc://' + b}
				temp_management_nodes.flatten!
				temp_management_nodes.reject! { |a| a =~ /#{@id}/ }
				new_temp_management_nodes = []
				temp_management_nodes.each do |temp|
					if @connected_management_nodes.detect {|cb| cb == temp }
					else
						new_temp_management_nodes << temp
					end
				end

				@management_nodes = new_temp_management_nodes

				self.parse_management_nodes
			end
		end
	end

	#Connect to Management Nodes
	def connect
		@attach = $ctx.socket(ZMQ::SUB)
		@attach.setsockopt(ZMQ::SUBSCRIBE,'heartbeat')

		Thread.new do
			loop do

				begin
					@management_nodes.each do |management_node|
puts "#{@id.inspect} MONITOR::CONNECT LOOP::MANAGEMENT NODE CONNECTING TO: #{management_node}"
						@attach.connect(management_node)
						@connected_management_nodes << management_node
						@management_nodes.delete(management_node)
					end
				rescue => error
					puts error
					puts "#{@id.inspect} MONITOR::CONNECT LOOP::UNABLE TO CONNECT TO: #{management_node}"
				end

				sleep 5
			end
		end
	end

	#Recieve JSON data
	def recieve
		Thread.new do
			@pass = true

			loop do
				if @connected_management_nodes != []
puts "#{@id.inspect} MONITOR::RECIEVE LOOP::PASS IS: #{@pass.inspect}"
					if @pass == false
puts "#{@id.inspect} MONITOR::RECIEVE LOOP::GETTING HEARTBEAT MESSAGE"
						message = @attach.recv
puts "#{@id.inspect} MONITOR::RECIEVE LOOP::RECIEVED HEARTBEAT TOPIC MESSAGE"
						message = @attach.recv if @attach.getsockopt(ZMQ::RCVMORE)
puts "#{@id.inspect} MONITOR::RECIEVE LOOP::RECIEVED HEARTBEAT MESSAGE"
						@results << message
					end

					@pass = false
				end
			end
		end
	end

	#Process JSON data
	def process
		Thread.new do
			req = $ctx.socket(ZMQ::REQ)

			loop do
puts "#{@id.inspect} MONITOR::PROCESS LOOP::STARTED"
				data = Crack::JSON.parse(@results.pop)
puts "#{@id.inspect} MONITOR::PROCESS LOOP::PROCESS DATA: #{data.inspect}"

				if data != nil
					if data['control_port'] != nil
puts "#{@id.inspect} MONITOR::PROCESS LOOP::CONTROL PORT CONNECT #{data['control_port']}"
						req.connect("tcp://*:#{data['control_port']}")
						sent = req.send("status?")
						response = nil
						response = req.recv if sent

						case response
						when "running"
puts "#{@id.inspect} MONITOR::PROCESS LOOP::RUNNING RESPONSE"
							sleep 1
						else
puts "#{@id.inspect} MONITOR::PROCESS LOOP::NO RESPONSE RECIEVED"
						end
					else
puts "#{@id.inspect} MONITOR::PROCESS LOOP::CONTROL PORT NIL: #{data['control_port'].inspect}"
					end
				else
puts "#{@id.inspect} MONITOR::PROCESS LOOP::DATA BLANK: #{data.inspect}"
				end
			end
		end
	end

	def close
		@attach.close
	end
end

class Management
  attr_accessor :self

	def initialize(args = {})
		@self = File.absolute_path(__FILE__)
	end

	def duplicate
		File.chmod(0755, @self)

		dup = Process.fork { exec @self }
		Process.detach(dup)
	end

	def self.close
		Thread.list.each do |t|
			t.exit unless t == Thread.current
		end

		$ctx.terminate
	end
end

if __FILE__ == $0
	hb = Heartbeat.new
	hb.announce
	hb.responder
	##hb.trap

	mon = Monitor.new({ :id => hb.id })
	mon.gather
	mon.connect
	mon.recieve
	mon.process

	before = Time.now.to_i + 300

	puts "starting..."

	while Time.now.to_i < before
	end

	puts "stopping..."

	mon.close
	hb.close
	Management.close
end