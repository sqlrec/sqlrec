-- Set random seed
math.randomseed(os.time())

-- Each thread will have its own copy of these variables
local current_request_log = false
local current_request_params = {}

function request()
    -- Generate random ID between 0-99999 (consistent with mock_data.py)
    local random_id = math.random(0, 99999)
    
    -- Construct request body
    local request_body = string.format('{"inputs":{"user_info":[{"id":%d}]},"params":{"recall_fun":"recall_fun"}}', random_id)
    
    -- Configure HTTP request
    wrk.method = "POST"
    wrk.headers["Content-Type"] = "application/json"
    wrk.body = request_body
    
    -- Determine if this request should be logged (1% probability)
    current_request_log = (math.random(1, 100) == 1)
    
    -- Store request parameters
    current_request_params = {
        method = wrk.method,
        path = wrk.path,
        content_type = wrk.headers["Content-Type"],
        body = request_body,
        user_id = random_id,  -- Extract user_id as a separate parameter
        recall_fun = "recall_fun"  -- Extract recall_fun parameter
    }
    
    return wrk.format()
end

-- Response handler to print response if the corresponding request was logged
function response(status, headers, body)
    -- Check if this response corresponds to a request that should be logged
    if current_request_log then
        print("Request:")
        print("Method: " .. current_request_params.method)
        print("URL: " .. current_request_params.path)
        print("Headers: Content-Type=" .. current_request_params.content_type)
        print("Body: " .. current_request_params.body)
        print()
        print("Response:")
        print("Status: " .. status)
        print("Body: " .. body)
        print("----------------------------------------")
        print()
    end
    
    -- Clear the request info to prepare for the next request
    current_request_log = false
    current_request_params = {}
end