<?php
/**
 * Created by PhpStorm.
 * User: n.pashkevich
 * Date: 22.06.2020
 * Time: 15:07
 */

namespace Kdteam\ElasticSearchClient;

use Elasticsearch\ClientBuilder;
use Exception;

class ElasticSearchClient
{

    private $strIndexName;
    private $elasticClient;
    private $logClient;
    private $baseParams = [];

    public function __construct($indexName, $host, $port, $baseParams)
    {
        if ($indexName && $host && $port) {
            $this->strIndexName = $indexName;
            $this->elasticClient = ClientBuilder::create()->setHosts(
                [
                    $host . ':' . $port
                ]
            )->build();
            $this->logClient = new CliEcho();
            $this->baseParams = $baseParams;
        } else {
            new Exception("ERROR, invalid input data!");
        }
    }

    public function toLog($text, array $arLogData, $date = false, $withOutput = false, $status = STATUS_CONSOLE_SUCCESS)
    {
        $arLogData['message'] = '(' . ($date ? $date : date('d.m.Y H:i:s')) . ') - ' . $text;
        if ($withOutput) {
            echo $this->logClient->toConsole($text, $status) . PHP_EOL;
        }
        return $this->toElastic($arLogData);
    }

    private function toElastic($arLogData)
    {
        date_default_timezone_set('UTC');
        $arEntryParams = $this->baseParams;
        $arEntryParams['timestamp'] = date('c');

        foreach ($arLogData as $k => $datum) {
            $arEntryParams[$k] = $datum;
        }

        $params = [
            'index' => $this->strIndexName,
            'type' => 'my_type',
            'body' => $arEntryParams
        ];

        return $this->elasticClient->index($params);
    }
}
