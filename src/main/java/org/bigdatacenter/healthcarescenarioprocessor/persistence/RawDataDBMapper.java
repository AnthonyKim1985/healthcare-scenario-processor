package org.bigdatacenter.healthcarescenarioprocessor.persistence;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.TableCreationTask;

@Mapper
public interface RawDataDBMapper {
    @Select("INSERT OVERWRITE DIRECTORY #{dataExtractionTask.hdfsLocation} ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ${dataExtractionTask.query}")
    void extractData(@Param("dataExtractionTask") DataExtractionTask dataExtractionTask);

    @Select("CREATE TABLE IF NOT EXISTS ${tableCreationTask.dbAndHashedTableName} STORED AS ORC AS ${tableCreationTask.query}")
    void createTable(@Param("tableCreationTask") TableCreationTask tableCreationTask);
}