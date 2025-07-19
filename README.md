# Rinha de Backend 2025 - jairoandre/rb2025

## Descrição

Solução desenvolvida em Golang para o desafio Rinha de Backend 2025.

## Arquitetura

A aplicação utiliza a biblioteca padrão Golang **net/http** para criar uma instância de um servidor HTTP Web multiplexado (__NewServerMUX__).
Ao receber uma requisiçào de pagamento, o *handler* encaminha o evento para uma fila *redis*, os eventos são posteriormente processados de forma paralelizada com o uso de *channels* e *go coroutines*.

O consumidor de eventos encaminha as requisições para um processador externo *default*. Em caso de falha as requisições são encaminhadas para o processador *fallback*.

Se os dois processadores apresentarem falhas, o evento de pagamento é redirecionado para fila e o consumo de eventos é suspendido temporariamente para verificação das saúdes dos mesmos. Assim que qualquer um dos processadores se apresentar disponível, o consumo é retomado.

Os eventos de pagamento só são gravados na base de dados após a confirmação dos processadores externos.

## Componentes da Solução

- nginx (load balancer)
- backend em golang
- postgres para persistência dos dados
- redis para mensageria