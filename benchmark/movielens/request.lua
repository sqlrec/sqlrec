-- Set random seed
math.randomseed(os.time())

function request()
    -- MovieLens-1M user IDs are 1 through 6040.
    local random_id = math.random(1, 6040)
    
    -- Construct request body
    local request_body = string.format('{"data":{"user_info":[{"user_id":%d}]},"params":{"recall_fun":"recall_fun","use_recall_service":"false","rank_fun":"rank_fun_simple"}}', random_id)
    
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
