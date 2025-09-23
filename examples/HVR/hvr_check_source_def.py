print("\n🔎 Testing adapt/check against source DB for a single table")
try:
    check_url = f"{base_url}/api/v6.1.0.36/hubs/{hub_name}/channels/{channel_name}/locs/{source_location}/adapt/check/{table_name}"
    check_payload = {
        "localize_datatypes": True,
        "fetch_extra": ["db_stats"]
    }
    print(f"POST {check_url}")
    print(f"Payload: {json.dumps(check_payload, indent=2)}")
    check_resp = requests.post(check_url, json=check_payload, headers=client._get_auth_headers(), verify=False)
    print(f"Status: {check_resp.status_code}")
    check_resp.raise_for_status()
    check_data = check_resp.json()
    print("\n📄 Adapt/Check (single table) response:")
    # Print a compact summary of key fields if present
    summary = {
        "schema": check_data.get("schema"),
        "base_name": check_data.get("base_name"),
        "exists_in_db": check_data.get("exists_in_db"),
        "diff": check_data.get("diff"),
    }
    print(json.dumps(summary, indent=2))
    # Print the first few columns if available
    cols = check_data.get("cols", [])
    if isinstance(cols, list) and cols:
        print("\nColumns (sample up to 5):")
        sample_cols = cols[:5]
        print(json.dumps(sample_cols, indent=2))
    elif isinstance(cols, dict) and cols:
        print("\nColumns (dict keys sample up to 5):")
        sample_cols = dict(list(cols.items())[:5])
        print(json.dumps(sample_cols, indent=2))
    else:
        print("No column details returned.")
except Exception as e:
    print(f"Error during per-table adapt/check test: {e}")
    import traceback
    traceback.print_exc()
