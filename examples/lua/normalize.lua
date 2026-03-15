local ctx = ...
local upstream = ctx.stageInput or {}
local inputRecords = upstream.records or {}
local outputRecords = {}

for i = 1, #inputRecords do
  local row = inputRecords[i]
  local textValue = row.text or ""
  outputRecords[#outputRecords + 1] = {
    id = row.id,
    text = tostring(textValue),
    normalized = true
  }
end

if #outputRecords == 0 then
  Datagen.emitWarning("lua.normalize.empty", "No records found in stageInput.records")
end

return {
  records = outputRecords
}

