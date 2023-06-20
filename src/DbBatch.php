<?php
/**
 * Created by PhpStorm.
 * User: Kyle
 * Date: 2021/7/1
 * Time: 11:10 AM
 */

namespace Pengyuanhong\YiiDbBatch;


class DbBatch
{
	/**
	 * 批量插入
	 * @param string $tableName 表名
	 * @param array $fields 要插入的字段名  ['name', 'gender', 'age']
	 * @param array $multipleData  要插入的字段值  [['name1', 1, 25], ['name2', 2, 30]]
	 * @return bool|string
	 */
	public static function insertBatch(string $tableName, array $fields, array $multipleData){
		try {
			// 成功返回记录数
			\Yii::$app->getDb()->createCommand()->batchInsert($tableName, $fields, $multipleData)->execute();
			return true;
		} catch (\Exception $e) {
			return $e->getMessage();
		}
	}
	
	/**
	 * 批量更新
	 * @param string $tableName 表名
	 * @param array $multipleData  [
						['id' => 1, 'name' => 'name_1', 'age' => 28],
						['id' => 2, 'name' => 'name_2', 'age' => 33],
					]
	 *              参数第一位id为主键（即：更新条件 where id = ...）
	 * @return bool
	 * @return bool|string
	 */
	public static function updateBatch(string $tableName, array $multipleData)
	{
		try {
			if (empty($multipleData)) {
				throw new \Exception("数据不能为空");
			}
			
			$firstRow  = current($multipleData);
			$updateColumn = array_keys($firstRow);
			// 默认以id为条件更新，如果没有ID则以第一个字段为条件
			$referenceColumn = isset($firstRow['id']) ? 'id' : current($updateColumn);
			unset($updateColumn[0]);
			
			// 拼接sql语句
			$updateSql = "UPDATE " . $tableName . " SET ";
			$sets      = [];
			foreach ($updateColumn as $uColumn) {
				$setSql = "`" . $uColumn . "` = CASE ";
				foreach ($multipleData as $data) {
					$setSql .= "WHEN `" . $referenceColumn . "` = ". $data[$referenceColumn] ." THEN '". $data[$uColumn] ."' ";
				}
				$setSql .= "ELSE `" . $uColumn . "` END ";
				$sets[] = $setSql;
			}
			$updateSql .= implode(', ', $sets);
			
			$referenceValues = [];
			foreach ($multipleData as $k => $v){
				$referenceValues[] = $v[$referenceColumn];
			}
			$whereIn = $referenceValues;
			$whereIn = implode(',', $whereIn);
			
			$updateSql = rtrim($updateSql, ", ") . " WHERE `" . $referenceColumn . "` IN (" . $whereIn . ")";
			// 返回成功记录数（如果没有要更新的数据，返回0）
			\Yii::$app->getDb()->createCommand($updateSql)->execute();
			return true;
		} catch (\Exception $e) {
			return $e->getMessage();
		}
	}
}
