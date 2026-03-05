import happybase
import json
from datetime import datetime
import time

print("🔥 Connecting to HBase...")

# Connect to HBase (running in Docker)
connection = happybase.Connection('localhost', 9090, autoconnect=False)

try:
    connection.open()
    print("✅ Connected to HBase successfully!")
    
    # Create tables if they don't exist
    tables = connection.tables()
    print(f"Existing tables: {tables}")
    
    # 1. User Sessions Table (time-series data)
    table_name = 'user_sessions'
    if table_name.encode() not in tables:
        print(f"\n📁 Creating table: {table_name}")
        connection.create_table(
            table_name,
            {
                'session': dict(),  # Session info column family
                'page_views': dict(),  # Page views column family
                'cart': dict()  # Cart contents column family
            }
        )
        print(f"✅ Table {table_name} created")
    else:
        print(f"✅ Table {table_name} already exists")
    
    # 2. Product Metrics Table (daily performance)
    table_name = 'product_metrics'
    if table_name.encode() not in tables:
        print(f"\n📁 Creating table: {table_name}")
        connection.create_table(
            table_name,
            {
                'daily': dict(),  # Daily metrics
                'metrics': dict()  # Other metrics
            }
        )
        print(f"✅ Table {table_name} created")
    else:
        print(f"✅ Table {table_name} already exists")
    
    # Load session data into HBase
    print("\n📁 Loading session data into HBase...")
    
    # Read sessions.json (one JSON object per line)
    sessions = []
    with open('sessions.json', 'r') as f:
        for line in f:
            try:
                sessions.append(json.loads(line))
            except:
                continue
    
    print(f"✅ Loaded {len(sessions)} sessions from file")
    
    # Get user_sessions table
    table = connection.table('user_sessions')
    
    # Insert sample sessions (first 100 for demo)
    batch = table.batch(batch_size=100)
    count = 0
    
    for session in sessions[:100]:  # Limit to 100 for demo
        # Create row key: user_id + reverse timestamp for latest first
        try:
            timestamp = datetime.fromisoformat(session['start_time'].replace('Z', ''))
            reverse_ts = 99999999999999 - int(timestamp.timestamp() * 1000)
            row_key = f"{session['user_id']}_{reverse_ts}".encode()
            
            # Store session data
            batch.put(row_key, {
                b'session:id': session['session_id'].encode(),
                b'session:device': session['device'].encode(),
                b'session:referrer': session['referrer'].encode(),
                b'session:start_time': session['start_time'].encode(),
                b'session:end_time': session['end_time'].encode(),
                b'session:conversion_status': session['conversion_status'].encode(),
                b'session:num_views': str(len(session['page_views'])).encode()
            })
            
            # Store first 5 page views as separate columns
            for i, view in enumerate(session['page_views'][:5]):
                view_data = f"{view['page_type']}|{view.get('product_id', '')}|{view['view_duration']}"
                batch.put(row_key, {
                    f'page_views:view_{i}'.encode(): view_data.encode()
                })
            
            count += 1
            if count % 20 == 0:
                print(f"  ✅ {count} sessions loaded...")
                batch.send()
                batch = table.batch(batch_size=100)
        except Exception as e:
            print(f"  ⚠️ Error loading session: {e}")
            continue
    
    if count % 20 != 0:
        batch.send()
    print(f"✅ Successfully loaded {count} sessions into HBase")
    
    # Query examples
    print("\n" + "="*60)
    print("🔍 HBase QUERY EXAMPLES")
    print("="*60)
    
    # Example 1: Get all sessions for a specific user
    print("\n📊 Example 1: Get all sessions for user_000001")
    user_id = 'user_000001'
    row_prefix = user_id.encode()
    
    scanner = table.scan(row_prefix=row_prefix)
    user_sessions = list(scanner)[:3]  # Get first 3
    
    print(f"Found {len(user_sessions)} sessions for {user_id}")
    for row_key, data in user_sessions:
        print(f"\n  Session: {data.get(b'session:id', b'').decode()}")
        print(f"    Device: {data.get(b'session:device', b'').decode()}")
        print(f"    Status: {data.get(b'session:conversion_status', b'').decode()}")
        print(f"    Start: {data.get(b'session:start_time', b'').decode()}")
    
    # Example 2: Get latest sessions (using reverse timestamp)
    print("\n📊 Example 2: Get 5 most recent sessions across all users")
    scanner = table.scan(limit=5)
    for row_key, data in scanner:
        user = row_key.decode().split('_')[0]
        print(f"\n  User: {user}")
        print(f"    Session: {data.get(b'session:id', b'').decode()}")
        print(f"    Status: {data.get(b'session:conversion_status', b'').decode()}")
        print(f"    Start: {data.get(b'session:start_time', b'').decode()}")
    
    # Example 3: Get sessions with conversion
    print("\n📊 Example 3: Get converted sessions (filter)")
    try:
        scanner = table.scan(filter="SingleColumnValueFilter('session', 'conversion_status', =, 'binary:converted')")
        converted = list(scanner)[:3]
        print(f"Found {len(converted)} converted sessions:")
        for row_key, data in converted:
            print(f"  User: {row_key.decode().split('_')[0]}")
            print(f"    Session: {data.get(b'session:id', b'').decode()}")
    except Exception as e:
        print(f"  ⚠️ Filter example not supported: {e}")
        print("  Showing alternative - scan all and filter manually:")
        scanner = table.scan(limit=20)
        converted_count = 0
        for row_key, data in scanner:
            if data.get(b'session:conversion_status', b'') == b'converted':
                converted_count += 1
                print(f"  User: {row_key.decode().split('_')[0]} - Session: {data.get(b'session:id', b'').decode()}")
                if converted_count >= 3:
                    break
    
    # Example 4: Count sessions by status
    print("\n📊 Example 4: Count sessions by conversion status")
    scanner = table.scan()
    status_count = {}
    total = 0
    for row_key, data in scanner:
        status = data.get(b'session:conversion_status', b'').decode()
        status_count[status] = status_count.get(status, 0) + 1
        total += 1
        if total >= 500:  # Limit for performance
            break
    
    for status, count in status_count.items():
        print(f"  {status}: {count} sessions ({count/total*100:.1f}%)")
    
    connection.close()
    print("\n✅ HBase implementation complete!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    print("\n💡 Make sure Docker is running and HBase container is started:")
    print("   docker start hbase-docker")