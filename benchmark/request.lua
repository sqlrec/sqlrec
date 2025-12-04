-- Set random seed
math.randomseed(os.time())

function request()
    -- Generate random ID between 0-99999 (consistent with mock_data.py)
    local random_id = math.random(0, 99999)
    
    -- Construct request body
    local request_body = string.format('{"inputs":{"user_info":[{"id":%d}]},"params":{"recall_fun":"recall_fun"}}', random_id)
    
    -- Configure HTTP request
    wrk.method = "POST"
    wrk.headers["Content-Type"] = "application/json"
    wrk.body = request_body
    
    return wrk.format()
end

-- Response handler to print response if the corresponding request was logged
function response(status, headers, body)
    current_request_log = (math.random(1, 100) == 1)
    if current_request_log then
        print("Response:")
        print("Status: " .. status)
        print("Body: " .. body)
        print("----------------------------------------")
        print()
    end
end