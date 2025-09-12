-- Search Measure Usage in ActivRule
SELECT 
	ar.ModuleName,
	rgl.LabelName AS FolderName,
	rgl.LabelType AS FolderType,
	ar.RuleGroupContent_RuleGroupText AS ActiveRule
FROM active_rules ar
JOIN rule_groups_labels rgl
	ON ar.RuleGroupLabelId = rgl.Id
WHERE ar.RuleGroupContent_RuleGroupText LIKE '%Activity Lead Time%';

-- Search Measure Usage in Procedure
SELECT 
	proc.ModuleName,
	proc.RuleGroupName AS ProcedureName,
	rgl.LabelName AS ProcedureFolder,
	rgl.LabelType AS FolderType,
	proc.RuleGroupContent_RuleGroupText AS Proc_Codes
FROM procedures proc
JOIN rule_groups_labels rgl
	ON proc.RuleGroupLabelId = rgl.Id
WHERE proc.RuleGroupContent_RuleGroupText LIKE '%Activity Lead Time%';

-- Search Measure Usage in ActionButton
SELECT
	abmu.ModuleName,
	measures.MeasureName,
	abmu.IsInput,
	abmu.IsOutput,
	ab.Name AS ActionButtonName,
	abir.ConfigJson_IBPLRules_template AS IBPLRules
FROM action_button_measure_usages abmu
JOIN action_buttons ab
	ON abmu.ActionButtonId = ab.Id
JOIN measures
 ON abmu.MeasureId = measures.Id
JOIN action_buttons_ibpl_rules abir
	ON ab.Id = abir.Id
WHERE measures.MeasureName LIKE '%CML Iteration Slice Level%';

-- Search Widget Dependency
SELECT
    vwd.ModuleName,
    vwd.Name AS WidgetName,
    views.Name AS ViewName,
    p.Name AS PageName,
    pg.Name AS PageGroupName,
    ws.Name AS WorkspaceName
FROM view_widget_definitions vwd
JOIN views
    ON vwd.ViewId = views.Id
JOIN pages p
    ON views.PageId = p.Id
JOIN page_groups pg
    ON p.PageGroupId = pg.Id
JOIN workspaces ws
    on ws.Id = views.WorkspaceId
WHERE vwd.Name LIKE '%Procurement Plan - COC Summary by Item%';

--Search Measure usage in widgets
SELECT
    wd.ModuleName,
    wd.Name,
    wd.WidgetType,
    wd.CreatedUserId,
    wd.ModifiedUserId,
    wma.ConfigJson_RegularMeasures_Name AS MeasureName
FROM widget_definitions wd
JOIN widget_model_axis wma
    ON wd.WidgetModelId = wma.Id
WHERE wma.ConfigJson_RegularMeasures_Name IS NOT NULL;


--Search ui preferences
SELECT
    up.ModuleName,
    up.PreferenceName,
    up.PreferenceType,
    up.ConfigJson_ModelDefinition_LevelAttributes_AttributeName AS AttributeName,
    up.ConfigJson_ModelDefinition_LevelAttributes_DimensionName AS DimensionName
FROM ui_preferences up;


-- Get LHS and RHS dependency in Procs, ARs, ABs
SELECT * FROM (
    SELECT
        rgmu.ModuleName,
        m.MeasureName,
        rgl.LabelType AS EntityType,
        rgl.LabelName AS EntityName,
        rgmu.IsOutput AS LHS,
        rgmu.IsInput AS RHS,
        ar.RuleGroupContent_RuleGroupText AS Script
    FROM rule_group_measure_usages rgmu
    JOIN active_rules ar
        ON ar.Id = rgmu.RuleGroupId
    JOIN measures m
        ON m.Id = rgmu.MeasureId
    JOIN rule_groups_labels rgl
        ON ar.RuleGroupLabelId = rgl.Id

    UNION ALL

    SELECT
        rgmu.ModuleName,
        m.MeasureName,
        rgl.LabelType AS EntityType,
        proc.RuleGroupName AS EntityName,
        rgmu.IsOutput AS LHS,
        rgmu.IsInput AS RHS,
        proc.RuleGroupContent_RuleGroupText AS Script
    FROM rule_group_measure_usages rgmu
    JOIN procedures proc
        ON proc.Id = rgmu.RuleGroupId
    JOIN measures m
        ON m.Id = rgmu.MeasureId
    JOIN rule_groups_labels rgl
        ON proc.RuleGroupLabelId = rgl.Id

    UNION ALL

    SELECT
        abmu.ModuleName,
        m.MeasureName,
        'ActionButton' AS EntityType,
        ab.Name AS EntityName,
        abmu.IsOutput AS LHS,
        abmu.IsInput AS RHS,
        abir.ConfigJson_IBPLRules_template AS IBPLRules
    FROM action_button_measure_usages abmu
    JOIN action_buttons ab
        ON abmu.ActionButtonId = ab.Id
    JOIN measures m
     ON abmu.MeasureId = m.Id
    JOIN action_buttons_ibpl_rules abir
        ON ab.Id = abir.Id

) WHERE MeasureName = 'W Procurement Forecast';

