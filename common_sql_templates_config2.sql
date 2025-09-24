-- Search Namednode
SELECT RuleGroupContent_NamedNodeName AS named_node_name,
       RuleGroupContent_RuleGroupText AS rule_group_text
FROM named_node nn
WHERE nn.RuleGroupContent_NamedNodeName = 'Retail_MFP_BU_Attribute_NN';

-- Search Namedset
SELECT named_set.RuleGroupName as named_set_name,
       named_set.RuleGroupContent_RuleGroupText as named_set_text
from named_set
WHERE named_set.RuleGroupName = 'PAG' or named_set.RuleGroupName = 'RE2ECountries';


-- Search Measure Usage in ActiveRule
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

--Search Widget Definitions by Title
SELECT
    wd.ModuleName,
    wd.Name,
    vwd.Title,
    wma.ConfigJson_LevelAttributes_DimensionName || '.[' ||
    wma.ConfigJson_LevelAttributes_AttributeName || ']' AS `Dimension.AttributeName`,
    up.ConfigJson_ModelDefinition_LevelAttributes_DimensionName || '.[' ||
    up.ConfigJson_ModelDefinition_LevelAttributes_AttributeName || ']' AS `UPDimensionName.UPAttributeName`,
    wma.ConfigJson_RegularMeasures_Name AS MeasureName
FROM widget_definitions wd
LEFT JOIN widget_model_axis wma
    ON wd.WidgetModelId = wma.Id
LEFT JOIN ui_preferences up
    ON wma.ConfigJson_LevelAttributes_AttributeName = up.PreferenceName
LEFT JOIN view_widget_definitions vwd
    ON vwd.Name = wd.Name
WHERE (vwd.Title = 'Currency Exchange Monthly' OR vwd.Name = '') AND (wma.ConfigJson_LevelAttributes_Axis <> 'none' OR wma.ConfigJson_RegularMeasures_Name IS NOT NULL)
GROUP BY
    wd.ModuleName,
    wd.Name,
    vwd.Title,
    `Dimension.AttributeName`,
    `UPDimensionName.UPAttributeName`,
    MeasureName;

--Search Widget Definitions by Name
SELECT
    wd.ModuleName,
    wd.Name,
    wma.ConfigJson_LevelAttributes_DimensionName || '.[' ||
    wma.ConfigJson_LevelAttributes_AttributeName || ']' AS `Dimension.AttributeName`,
    up.ConfigJson_ModelDefinition_LevelAttributes_DimensionName || '.[' ||
    up.ConfigJson_ModelDefinition_LevelAttributes_AttributeName || ']' AS `UPDimensionName.UPAttributeName`,
    wma.ConfigJson_RegularMeasures_Name AS MeasureName
FROM widget_definitions wd
         LEFT JOIN widget_model_axis wma
                   ON wd.WidgetModelId = wma.Id
         LEFT JOIN ui_preferences up
                   ON wma.ConfigJson_LevelAttributes_AttributeName = up.PreferenceName
WHERE wd.Name = 'LP & BU Reconciliation' AND (wma.ConfigJson_LevelAttributes_Axis <> 'none' OR wma.ConfigJson_RegularMeasures_Name IS NOT NULL)
GROUP BY
    wd.ModuleName,
    wd.Name,
    `Dimension.AttributeName`,
    `UPDimensionName.UPAttributeName`,
    MeasureName;


--Search ui preferences
SELECT
    up.ModuleName,
    up.PreferenceName,
    up.PreferenceType,
    up.ConfigJson_ModelDefinition_LevelAttributes_DimensionName AS DimensionName,
    up.ConfigJson_ModelDefinition_LevelAttributes_AttributeName AS AttributeName
FROM ui_preferences up
WHERE up.PreferenceName = 'Data Mgmt Selling Season UIP';


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

