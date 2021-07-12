<?php
/**
 * Created by PhpStorm.
 * User: n.pashkevich
 * Date: 22.06.2020
 * Time: 15:07
 */

namespace Kdteam\ElasticSearchClient;

use Elasticsearch\ClientBuilder;
use Kdteam\CliOutput\CliOutput;
use Exception;

class ElasticSearchClient
{

    private $strIndexName;
    private $elasticClient;
    private $logClient;
    private $baseParams = [];
    private $cliMode = true;

    public function __construct($indexName, $host, $port, $baseParams, $cli = true)
    {
        if ($indexName && $host && $port) {
            $this->strIndexName = $indexName;
            $this->elasticClient = ClientBuilder::create()->setHosts(
                [
                    $host . ':' . $port
                ]
            )->build();
            $this->logClient = new CliOutput();
            $this->baseParams = $baseParams;
            $this->cliMode = $cli;
        } else {
            new Exception("ERROR, invalid input data!");
        }
    }

    public function toLog(
        $text,
        array $arLogData,
        $date = false,
        $sendToElastic = false,
        $status = STATUS_CONSOLE_SUCCESS
    ) {
        $arLogData['message'] = '(' . ($date ? $date : date('d.m.Y H:i:s')) . ') - ' . $text;
        if ($this->cliMode) {
            $this->logClient->toConsole($text, $status) . PHP_EOL;
        }
        if ($sendToElastic) {
            if (strpos($_SERVER['HOSTNAME'], '.kdteam.su') === false) {
                return $this->toElastic($arLogData);
            } else {
                return true;
            }
        }
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
            'body' => $arEntryParams,
            'type' => '_doc'
        ];

        return $this->elasticClient->index($params);
    }
}
