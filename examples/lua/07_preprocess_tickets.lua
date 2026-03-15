local ctx = ...
local inputRecords = ctx.stageInput or {}
local normalized = {}
local highHintCount = 0
local missingContentCount = 0

for i = 1, #inputRecords do
  local row = inputRecords[i] or {}
  local ticketId = Datagen.get(row, "id", Datagen.textTemplate("ticket-$1", {}, i))
  local title = Datagen.trim(Datagen.get(row, "title", ""))
  local body = Datagen.trim(Datagen.get(row, "body", ""))
  local product = Datagen.lower(Datagen.get(row, "product", "general"))
  local priorityHint = Datagen.lower(Datagen.get(row, "priorityHint", "medium"))

  local metaRaw = Datagen.get(row, "meta.raw_json", nil)
  if metaRaw ~= nil then
    local parsedMeta = Datagen.fromJson(metaRaw, {})
    if Datagen.has(parsedMeta, "severity") then
      priorityHint = Datagen.lower(Datagen.get(parsedMeta, "severity", priorityHint))
    end
  end

  if priorityHint == "high" then
    highHintCount = highHintCount + 1
  end

  if title == "" or body == "" then
    missingContentCount = missingContentCount + 1
    Datagen.emitWarning(
      "lua.preprocess.missing_text",
      Datagen.textTemplate("ticket {id} is missing title/body content", { id = ticketId })
    )
  end

  normalized[#normalized + 1] = {
    id = tostring(ticketId),
    title = title,
    body = body,
    product = product,
    productLabel = Datagen.upper(product),
    priorityHint = priorityHint,
    slug = Datagen.slug(title)
  }
end

if #normalized == 0 then
  Datagen.emitWarning("lua.preprocess.empty", "No tickets were found in pipeline input")
end

local stats = {
  inputCount = #inputRecords,
  highHintCount = 0,
  missingContentCount = 0
}
stats = Datagen.set(stats, "highHintCount", highHintCount)
stats = Datagen.set(stats, "missingContentCount", missingContentCount)

local auditJson = Datagen.toJson({
  source = "pipeline_input",
  stage = ctx.stageIdentifier
})
local audit = Datagen.fromJson(auditJson, {})

return {
  tickets = normalized,
  stats = stats,
  audit = audit
}
