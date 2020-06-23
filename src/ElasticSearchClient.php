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

    public function __construct($indexName, $host, $port)
    {
        if ($indexName && $host && $port) {
            $this->strIndexName = $indexName;
            $this->elasticClient = ClientBuilder::create()->setHosts(
                [
                    $host . ':' . $port
                ]
            )->build();
        } else {
            new Exception("ERROR, invalid input data!");
        }
    }

    public function toLog($text, array $arLogData)
    {
        $arLogData['message'] = $text;
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
