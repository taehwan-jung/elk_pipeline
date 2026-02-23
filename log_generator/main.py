import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

#  Kafka setting
producer = KafkaProducer(
    bootstrap_servers=['localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1
)

#  Topics
TOPICS = {
    'login': 'logs-login',
    'payment': 'logs-payment',
    'click': 'logs-click'
}

def generate_fake_log(log_type):
    # Device(User Agent) list
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0", # PC
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15", # Mobile
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/537.36", # Mac
    ]
    
    user_ids = [f"user_{i}" for i in range(1, 10000)]
    
    # Base log
    base_log = {
        'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 
        'user_id': random.choice(user_ids),
        'log_type': log_type,
        'ip_address': f"192.168.1.{random.randint(1, 255)}"
    }

    # Login log
    if log_type == 'login':
        status = 'success' if random.random() > 0.1 else 'fail'
        ua = random.choice(user_agents) if random.random() > 0.05 else "Python-requests/2.31.0"
        
        base_log.update({
            'method': random.choice(['email', 'google', 'apple']), 
            'status': status,
            'user_agent': ua
        })

    # Payment log
    elif log_type == 'payment':
        base_log.update({
            'amount': random.randint(1000, 100000), 
            'item_id': f"item_{random.randint(1, 50)}", 
            'currency': 'KRW',
            'user_agent': random.choice(user_agents),
            'status': 'success' 
        })

    # Click log
    elif log_type == 'click':
        base_log.update({
            'page_url': f"/products/{random.randint(1, 100)}", 
            'button_id': random.choice(['buy_now', 'add_cart', 'wishlist']),
            'user_agent': random.choice(user_agents)
        })
        
    return base_log

def main():
    print("🚀 og Generator started.")
    try:
        while True:
            # 3. Determine log type
            log_type = random.choice(list(TOPICS.keys()))
            topic_name = TOPICS[log_type]
            
            log_data = generate_fake_log(log_type)
            
            producer.send(topic_name, log_data)
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] at {topic_name} succeed: {log_data['user_id']}")
            
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("\n👋 Stop log generating.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()