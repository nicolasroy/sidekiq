require 'sidekiq'


module Sidekiq
  module Scheduled
    class Timer
      MONITOR_INTERVAL = 15
      TIMEOUT = 90

      def initialize(mng)
        @mng = mng
      end

      def start_monitor
        add_jitter
        monitor
      end

      private

      def monitor
        workers.each do |msg|
          time = msg['run_at'].is_a?(Numeric) ? Time.at(msg['run_at']) : Time.parse(msg['run_at'])

          if Time.now - time > timeout
            @mng.stop

            @mng.start
            return
          end
        end

        after(monitor_interval) { monitor }
      end

      def monitor_interval
        Sidekiq.options[:monitor_interval] || MONITOR_INTERVAL
      end

      def timeout
        Sidekiq.options[:timeout] || TIMEOUT
      end

      def workers
        @workers ||= begin
          Sidekiq.redis do |conn|
            conn.smembers('workers').map do |w|
              msg = conn.get("worker:#{w}")
              msg ? [w, Sidekiq.load_json(msg)] : nil
            end.compact.sort { |x| x[1] ? -1 : 1 }
          end
        end
      end

      def add_jitter
        begin
          sleep(poll_interval * rand)
        rescue Celluloid::Task::TerminatedError
          # Hit Ctrl-C when Sidekiq is finished booting and we have a chance
          # to get here.
        end
      end
    end
  end
end