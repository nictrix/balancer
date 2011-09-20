#!/usr/bin/env ruby

require 'thread'
require 'zmq'
require 'crack'

Thread.abort_on_exception = true
$ctx = ZMQ::Context.new

class Heartbeat
  attr_accessor :id, :thread, :base, :base_id, :port, :control_port, :attach

	def initialize(args = {})
		@id = args[:id] || Heartbeat.id
		@thread = args[:inproc] || 'me'
		@base = File.expand_path(args[:directory] || '/tmp/brothers')
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

		if File.exists?(@base_id) == false
			if File.directory?(@base) == false
				Dir.mkdir(@base)
			end
		else
			@base_id = @base + "/#{@id}"
		end

		File.open(@base_id, 'w') {|f| f.write(Process.pid) }

puts "BINDING TO PORT #{@port}"

		counter = 0
		begin
			@attach.bind("ipc://#{@base_id}")
			@attach.bind("inproc://#{@thread}")
			Thread.new { @attach.bind("tcp://*:#{@port}") }
		rescue => error
			@port = Heartbeat.port
			counter = counter + 1
			raise unless counter > 10
			retry
		end

puts "CREATING HEARTBEAT LOOP"

		Thread.new do
			loop do

puts "IN HEARTBEAT LOOP"

				@attach.send("{ 'id' : #{@id}, 'tcp' : #{@port}, 'file' : #{@base_id}, 'control_port' : #{@control_port} }")

puts "SENT MY #{@id}"

				sleep 1
			end
		end
	end

	#Control Port
	def responder

puts "BINDING TO CONTROL PORT #{@control_port}"

		Thread.new do
			resp = $ctx.socket(ZMQ::REP)

		counter = 0
		begin
			resp.bind("tcp://*:#{@control_port}")
		rescue => error
			@control_port = Heartbeat.control_port
			counter = counter + 1
			raise unless counter > 10
			retry
		end

			while message = resp.recv

puts "IN WHILE LOOP to RESPOND TO REQUESTS"

puts message

				case message
				when "status?"
puts					resp.send("running")

puts "RESPONDED TO A REQUEST"
				end
			end
		end

puts "FINISHED AT CONTROL PORT - LOOP IS RUNNING"

	end

	def trap
		Signal.list.each do |k,signal_type|
			Signal.trap(signal_type, "IGNORE")
		end
	end

	def close
		Thread.list.each do |t|
			t.exit unless t == Thread.current
		end

		@attach.close
	end
end

class Monitor
  attr_accessor :id, :base, :attach, :results, :brothers, :connected_brothers

	def initialize(args = {})
		@base = File.expand_path(args[:directory] || '/tmp/brothers')
		raise "I have no ID?" unless args[:id] != nil
		@id = args[:id]
		@attach = nil
		@recieve_thread = nil
		@results = Queue.new
		@brothers = []
		@connected_brothers = []
	end

	#Do I need to duplicate myself?
	def parse_brothers

#puts @brothers.inspect
#puts @connected_brothers.inspect

		sleep 25

		if @brothers == [] && @connected_brothers == []
			b = Brother.new
			b.duplicate

puts "OKAY I CALLED A DUPLICATE LETs wait"

			sleep 25
		end
	end

	#Gather All Available Brothers
	def gather
		Thread.new do
			loop do

#puts "GATHERING BROTHERS"

				temp_brothers = Dir[@base + '/**'].collect {|b| 'ipc://' + b}
				temp_brothers.flatten!
				temp_brothers.reject! { |a| a =~ /#{@id}/ }
				new_temp_brothers = []
				temp_brothers.each do |temp|

#puts temp.inspect

					if @connected_brothers.detect {|cb| cb == temp }

#puts "FOUND"

					else
						new_temp_brothers << temp
					end
				end

				@brothers = new_temp_brothers

				self.parse_brothers
			end
		end
	end

	#Connect to Brothers
	def connect

puts "CONNECTING TO BROTHERS"

		@attach = $ctx.socket(ZMQ::SUB)
		self.recieve

		Thread.new do
			loop do

puts "IN CONECT TO BROTHERS LOOP"

				begin
					@brothers.each do |brother|

puts "ATTACHING TO BROTHER #{brother}"

						@attach.connect(brother)
						@connected_brothers << brother
					end
				rescue => error
					puts error
					puts "UNABLE TO BIND! to #{brother}"
				end

puts "GOING TO SLEEP WHEN CONNECTED TO BROTHERS"
				sleep 5
			end
		end
	end

	#Recieve JSON data
	def recieve

puts "RECIEVING DATA"

		Thread.new do
			loop do
				if @connected_brothers != []

puts "GOT SOME CONNECTED BROTHERS"

					message = @attach.recv

puts "GOT A MESSAGE!!!!"
puts message

					@results << message
				end
			end
		end
	end

	#Process JSON data
	def process
		Thread.new do
			req = $ctx.socket(ZMQ::REQ)

			loop do

puts "GOING TO GET RESULTS..."

				data = Crack::JSON.parse(@results.pop)

puts "PROCESS DATA: #{data.inspect}"

				if data != nil
					if data['control_port'] != nil

puts "CONNECTING TO BROTHERs CONTROL PORT #{data['control_port']}"

						req.connect("tcp://*:#{data['control_port']}")
						req.send("status?")
						@response = nil
						response_thread = Thread.new { loop { @response = req.recv } }

puts @response

						case @response
						when "running"
							sleep 1
						else
							#dup
						end

						response_thread.exit
					else
						#dup
					end
				else
					#dup
				end
			end
		end
	end

	def close
		Thread.list.each do |t|
			t.exit unless t == Thread.current
		end

		@attach.close
	end
end

class Brother
  attr_accessor :self

	def initialize(args = {})
		@self = File.absolute_path(__FILE__)
	end

	def duplicate
		File.chmod(0755, @self)

		dup = Process.fork { exec @self }
		Process.detach(dup)
	end
end

if __FILE__ == $0
	hb = Heartbeat.new
	hb.announce
	hb.responder
	##hb.trap
	#
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
	$ctx.terminate
end