module Resque
  module Plugins
    module Loner
      class Helpers
        extend Resque::Helpers

        def self.loner_queued?(queue, item)
          return false unless item_is_a_unique_job?(item)
          redis.get(unique_job_queue_key(queue, item)) == "1"
        end

        def self.mark_loner_as_queued(queue, item)
          return unless item_is_a_unique_job?(item)
          key = unique_job_queue_key(queue, item)
          redis.set(key, 1)
          unless(ttl=item_ttl(item)) == -1 # no need to incur overhead for default value
            redis.expire(key, ttl)
          end
        end

        def self.mark_loner_as_unqueued(queue, job)
          item = job.is_a?(Resque::Job) ? job.payload : job
          return unless item_is_a_unique_job?(item)
          unless (ttl=loner_lock_after_execution_period(item)) == 0
            redis.expire(unique_job_queue_key(queue, item), ttl)
          else
            redis.del(unique_job_queue_key(queue, item))
          end
        end

        def self.unique_job_queue_key(queue, item)
          args = item[:args] || item['args']
          return 'args must be array' unless args.is_a?(Array)
          args_hash = args.first
          return 'Args must contain hash' unless args_hash.is_a?(Array)
          uniq_key = args_hash[:unique_key] || args_hash["unique_key"]
          "loners:queue:#{queue}:job:#{uniq_key}"
        end

        def self.item_is_a_unique_job?(item)
          args = item[:args] || item['args']
          return true if args.is_a?(Array) && (args.first[:uniq] || args.first['uniq'])
          klass = constantize(item[:class] || item["class"])
          klass.included_modules.include?(::Resque::Plugins::UniqueJob)
        rescue
          false # Resque testsuite also submits strings as job classes while Resque.enqueue'ing,
        end

        def self.item_ttl(item)
          begin
            constantize(item[:class] || item["class"]).loner_ttl
          rescue
            -1
          end
        end

        def self.loner_lock_after_execution_period(item)
          begin
            constantize(item[:class] || item["class"]).loner_lock_after_execution_period
          rescue
            0
          end
        end

        def self.job_destroy(queue, klass, *args)
          klass = klass.to_s
          redis_queue = "queue:#{queue}"

          redis.lrange(redis_queue, 0, -1).each do |string|
            json   = decode(string)

            match  = json['class'] == klass
            match &= json['args'] == args unless args.empty?

            if match
             Resque::Plugins::Loner::Helpers.mark_loner_as_unqueued( queue, json )
            end
          end
        end

        def self.cleanup_loners(queue)
          keys = redis.keys("loners:queue:#{queue}:job:*")
          redis.del(*keys) unless keys.empty?
        end

      end
    end
  end
end
