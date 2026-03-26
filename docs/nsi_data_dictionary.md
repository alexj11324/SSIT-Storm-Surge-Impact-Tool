# NSI Dataset Feature Dictionary (Parquet Version)

The NSI data in this project is sourced from the **USACE National Structure Inventory (2022)**. It provides point-level data for structures across the US, including structural characteristics and population estimates. The data is partitioned by State.

## 1. Identification & Location

| Feature Name | Meaning | Type | Details |
| :--- | :--- | :--- | :--- |
| **state** | **State Name** | STRING | **Partition Key**. The US State where the structure is located (e.g., `Delaware`). |
| **bid** | Building ID | STRING | Unique identifier assigned by NSI for each structure. |
| **x / longitude** | Longitude | DOUBLE | Longitude coordinate (WGS84). |
| **y / latitude** | Latitude | DOUBLE | Latitude coordinate (WGS84). |
| **cbfips** | Census Block FIPS | STRING | 15-digit Census Block code, useful for joining with Census demographic data. |
| **ftprntid** | Footprint ID | STRING | ID linking to external building footprint datasets (Microsoft/FEMA). |

## 2. Structural Characteristics

These features describe physical attributes essential for assessing flood vulnerability and potential damage.

| Feature Name | Meaning | Type | Details |
| :--- | :--- | :--- | :--- |
| **occtype** | **Occupancy Type** | STRING | Core classification. e.g., `RES1` (Single Family), `COM1` (Retail), `RES3` (Multi-Family). |
| **bldgtype** | Building Material | STRING | Construction material (e.g., Wood, Masonry, Concrete). |
| **num_story** | Number of Stories | INT | Estimated number of stories above ground. |
| **sqft** | Area (SqFt) | DOUBLE | Total floor area in square feet. |
| **found_type** | Foundation Type | STRING | e.g., `Slab`, `Crawl`, `Pile`, `Basement`. Critical for determining First Floor Elevation. |
| **found_ht** | **Foundation Height** | DOUBLE | Height of the first floor above the ground (in Feet). |
| **ground_elv** | **Ground Elevation** | DOUBLE | Elevation of the ground at the structure's location (in Feet). |
| **val_struct** | Structure Value | DOUBLE | Estimated replacement cost of the structure ($). |
| **val_cont** | Content Value | DOUBLE | Estimated value of contents inside the structure ($). |
| **st_damcat** | Damage Category | STRING | `RES` (Residential), `COM` (Commercial), `IND` (Industrial), `PUB` (Public). |

## 3. Population & Social Vulnerability

These features estimate the population present in the structure, crucial for Red Cross evacuation planning and impact analysis.

| Feature Name | Meaning | Type | Details |
| :--- | :--- | :--- | :--- |
| **pop2pmo65** | Night Pop (>65) | INT | Estimated population over 65 present at night (2 PM). |
| **pop2pmu65** | Night Pop (<65) | INT | Estimated population under 65 present at night. |
| **pop2amo65** | Day Pop (>65) | INT | Estimated population over 65 present during the day (2 AM). |
| **pop2amu65** | Day Pop (<65) | INT | Estimated population under 65 present during the day. |
| **o65disable** | Elderly Disability | DOUBLE | Probability/Rate of disability among the >65 population. |
| **u65disable** | General Disability | DOUBLE | Probability/Rate of disability among the <65 population. |

## 4. Risk Assessment Logic

For flood impact analysis, use the following logic:

> **First Floor Elevation (FFE) = Ground Elevation (ground_elv) + Foundation Height (found_ht)**

*   **Yard Inundation**: If `SLOSH Surge` > `ground_elv`. (The property is wet).
*   **Structure Inundation**: If `SLOSH Surge` > `FFE`. (Water enters the building, causing significant damage).

## 5. Metadata

*   **source**: Origin of the record (e.g., `HIFLD`, `Microsoft`, `CoreLogic`).
*   **firmzone**: FEMA Flood Insurance Rate Map zone (e.g., `AE`, `VE`, `X`).

---

# NSI Dataset Feature Dictionary (Parquet Version) — 中文版

本项目使用的 NSI 数据源自 **USACE National Structure Inventory (2022)**，是一个包含全美建筑物位置、结构特征及人口统计信息的综合数据库。数据已转换为 Parquet 格式并按州（State）分区。

## 1. 基础识别与位置信息


| 字段名称              | 含义                | 类型     | 详细说明                                      |
| ----------------- | ----------------- | ------ | ----------------------------------------- |
| **state**         | **州名**            | STRING | **分区字段**。建筑所在的州（如 `Delaware`, `Florida`）。 |
| **bid**           | 建筑唯一 ID           | STRING | NSI 为每个结构分配的唯一标识符。                        |
| **x / longitude** | 经度                | DOUBLE | 建筑物的经度坐标 (WGS84)。`x` 和 `longitude` 通常相同。  |
| **y / latitude**  | 纬度                | DOUBLE | 建筑物的纬度坐标 (WGS84)。`y` 和 `latitude` 通常相同。   |
| **cbfips**        | Census Block FIPS | STRING | 普查区块代码（15 位），用于关联人口普查数据。                  |
| **ftprntid**      | 足迹 ID             | STRING | 关联到微软或 FEMA 建筑足迹数据的 ID。                   |


## 2. 结构特征 (Structural Characteristics)

这些字段描述了建筑物的物理属性，对于评估洪水脆弱性至关重要。


| 字段名称           | 含义       | 类型     | 详细说明                                                        |
| -------------- | -------- | ------ | ----------------------------------------------------------- |
| **occtype**    | **占用类型** | STRING | 核心字段。如 `RES1` (单户住宅), `COM1` (商业), `RES2` (公寓)。             |
| **bldgtype**   | 建筑类型     | STRING | 建筑材料/构造类型（如木质、砖混）。                                          |
| **num_story**  | 楼层数      | INT    | 建筑物的估计层数。                                                   |
| **sqft**       | 面积       | DOUBLE | 建筑面积（平方英尺）。                                                 |
| **found_type** | 地基类型     | STRING | 如 `Slab` (板式), `Crawl` (爬行空间), `Pile` (桩基)。直接影响洪水破坏程度。      |
| **found_ht**   | **地基高度** | DOUBLE | 一楼地板相对于地面的高度（英尺）。**关键字段**：用于计算一楼海拔 (First Floor Elevation)。 |
| **ground_elv** | **地面海拔** | DOUBLE | 建筑物所在地的地面海拔（英尺）。                                            |
| **val_struct** | 结构价值     | DOUBLE | 建筑物的重置成本估值（美元）。                                             |
| **val_cont**   | 内容价值     | DOUBLE | 建筑物内部财产的估值（美元）。                                             |
| **st_damcat**  | 损坏类别     | STRING | `RES` (住宅), `COM` (商业), `IND` (工业), `PUB` (公共)。             |


## 3. 人口与社会脆弱性 (Population & Vulnerability)

这些字段估算了建筑物内的人口分布，这对于红十字会的人道主义救援规划（如疏散、物资分发）至关重要。


| 字段名称           | 含义          | 类型     | 详细说明                   |
| -------------- | ----------- | ------ | ---------------------- |
| **pop2pmo65**  | 晚间人口 (>65岁) | INT    | 晚上在该建筑内的 65 岁以上老年人口估算。 |
| **pop2pmu65**  | 晚间人口 (<65岁) | INT    | 晚上在该建筑内的 65 岁以下人口估算。   |
| **pop2amo65**  | 日间人口 (>65岁) | INT    | 白天在该建筑内的 65 岁以上老年人口估算。 |
| **pop2amu65**  | 日间人口 (<65岁) | INT    | 白天在该建筑内的 65 岁以下人口估算。   |
| **o65disable** | 残障老年人比例     | DOUBLE | 65岁以上人口中有残障的概率/比例。     |
| **u65disable** | 残障非老年人比例    | DOUBLE | 65岁以下人口中有残障的概率/比例。     |


## 4. 关键计算逻辑 (Risk Assessment)

在进行洪水风险分析时：

> **一楼海拔 (First Floor Elevation, FFE) = 地面海拔 (ground_elv) + 地基高度 (found_ht)**

- **淹没判断**：如果 `SLOSH 浪高 (Surge)` > `地面海拔 (ground_elv)`，则此时**院子被淹**。
- **入户判断**：如果 `SLOSH 浪高 (Surge)` > `一楼海拔 (FFE)`，则此时**水进入房屋内部**（造成结构和内容损失）。

## 5. 数据来源

- **source**: 数据来源标识（如 `HIFLD`, `CoreLogic`, `Microsoft`）。
- **firmzone**: FEMA 洪水保险费率图 (FIRM) 分区代码（如 `AE`, `X`）。
