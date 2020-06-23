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
    private $fileName;
    private $logClient;

    public function __construct($indexName, $host, $port, $fileName)
    {
        if ($indexName && $host && $port && $fileName) {
            $this->strIndexName = $indexName;
            $this->elasticClient = ClientBuilder::create()->setHosts(
                [
                    $host . ':' . $port
                ]
            )->build();
            $this->fileName = $fileName;
            $this->logClient = new CliEcho();
        } else {
            new Exception("ERROR, invalid input data!");
        }
    }

    public function toLog($text, array $arLogData, $date = false, $withOutput = false, $status = STATUS_CONSOLE_SUCCESS)
    {
        $arLogData['message'] = $this->fileName . ' (' . ($date ? $date : date('d.m.Y H:i:s')) . ') - ' . $text;
        if ($withOutput) {
            echo $this->logClient->toConsole($text, $status) . PHP_EOL;
        }
        return $this->toElastic($arLogData);
    }

    private function toElastic($arLogData)
    {
        date_default_timezone_set('UTC');
        $arEntryParams = [
            'timestamp' => date('c'),
        ];

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
