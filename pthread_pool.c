/*
 * Copyright(c) 2021-2024 All rights reserved by Heekuck Oh.
 * 이 프로그램은 한양대학교 ERICA 컴퓨터학부 학생을 위한 교육용으로 제작되었다.
 * 한양대학교 ERICA 학생이 아닌 이는 프로그램을 수정하거나 배포할 수 없다.
 * 프로그램을 수정할 경우 날짜, 학과, 학번, 이름, 수정 내용을 기록한다.
 */
#include <stdlib.h>
#include "pthread_pool.h"

/*
 * 풀에 있는 일꾼(일벌) 스레드가 수행할 함수이다.
 * FIFO 대기열에서 기다리고 있는 작업을 하나씩 꺼내서 실행한다.
 * 대기열에 작업이 없으면 새 작업이 들어올 때까지 기다린다.
 * 이 과정을 스레드풀이 종료될 때까지 반복한다.
 */
static void *worker(void *param)
{
    // 여기를 완성하세요
    pthread_pool_t *pool = (pthread_pool_t *)param;
    task_t task;
    while (true) {
        pthread_mutex_lock(&pool->mutex); // 대기열 접근을 위한 락 획득
        if (pool->q_len == 0 && pool->state != ON) {
            pthread_mutex_unlock(&pool->mutex);
            break; // 스레드풀이 종료 상태면 루프 탈출
        }
        while (pool->q_len == 0 && pool->state == ON) {
            pthread_cond_wait(&pool->full, &pool->mutex); // 대기열이 비어있으면 대기
        }
        if (pool->q_len == 0) {
            pthread_mutex_unlock(&pool->mutex);
            continue; // 대기열이 비어있으면 다시 루프 시작
        }
        task = pool->q[pool->q_front]; // 대기열에서 작업 꺼내기
        pool->q_front = (pool->q_front + 1) % pool->q_size; // 원형 버퍼 처리
        pool->q_len--; // 대기열 길이 감소
        pthread_cond_signal(&pool->empty); // 대기열에 빈 자리가 생겼음을 알림
        pthread_mutex_unlock(&pool->mutex); // 락 해제
        // 작업 실행
        if(task.function != NULL) {
            
            task.function(task.param); // 작업 함수가 NULL이 아니면 작업 실행
        }
    }
    pthread_exit(NULL); // 스레드 종료
}

/*
 * 스레드풀을 생성한다. bee_size는 일꾼(일벌) 스레드의 개수이고, queue_size는 대기열의 용량이다.
 * bee_size는 POOL_MAXBSIZE를, queue_size는 POOL_MAXQSIZE를 넘을 수 없다.
 * 일꾼 스레드와 대기열에 필요한 공간을 할당하고 변수를 초기화한다.
 * 일꾼 스레드의 동기화를 위해 사용할 상호배타 락과 조건변수도 초기화한다.
 * 마지막 단계에서는 일꾼 스레드를 생성하여 각 스레드가 worker() 함수를 실행하게 한다.
 * 대기열로 사용할 원형 버퍼의 용량이 일꾼 스레드의 수보다 작으면 효율을 극대화할 수 없다.
 * 이런 경우 사용자가 요청한 queue_size를 bee_size로 상향 조정한다.
 * 성공하면 POOL_SUCCESS를, 실패하면 POOL_FAIL을 리턴한다.
 */
int pthread_pool_init(pthread_pool_t *pool, size_t bee_size, size_t queue_size)
{
    if (bee_size > POOL_MAXBSIZE || queue_size > POOL_MAXQSIZE) {
        return POOL_FAIL; // 일꾼 스레드 수 또는 대기열 크기가 최대치를 초과
    }
    if (bee_size > queue_size) {
        queue_size = bee_size; // 대기열 크기를 일꾼 스레드 수로 조정
    }
    pool->bee_size = bee_size;
    pool->q_size = queue_size;
    pool->q_front = 0;
    pool->q_len = 0;
    pool->state = ON;
    pool->q = (task_t *)malloc(queue_size * sizeof(task_t));
    if (pool->q == NULL) {
        return POOL_FAIL; // 메모리 할당 실패
    }
    pool->bee = (pthread_t *)malloc(bee_size * sizeof(pthread_t));
    if (pool->bee == NULL) {
        free(pool->q);
        return POOL_FAIL; // 메모리 할당 실패
    }
    if (pthread_mutex_init(&pool->mutex, NULL) != 0) {
		free(pool->q);
		free(pool->bee);
		return POOL_FAIL;
	}
	if (pthread_cond_init(&pool->full, NULL) != 0) {
		pthread_mutex_destroy(&pool->mutex);
		free(pool->q);
		free(pool->bee);
		return POOL_FAIL;
	}
	if (pthread_cond_init(&pool->empty, NULL) != 0) {
		pthread_cond_destroy(&pool->full);
		pthread_mutex_destroy(&pool->mutex);
		free(pool->q);
		free(pool->bee);
		return POOL_FAIL;
	}
    for (size_t i = 0; i < bee_size; i++) {
        if (pthread_create(&pool->bee[i], NULL, worker, (void *)pool) != 0) {
            pool->state = OFF;
            for (size_t j = 0; j < i; j++) {
                pthread_join(pool->bee[j], NULL); // 이전에 생성된 스레드 조인
            }
            pthread_mutex_destroy(&pool->mutex);
            pthread_cond_destroy(&pool->full);
            pthread_cond_destroy(&pool->empty);
            free(pool->q);
            free(pool->bee);
            return POOL_FAIL; // 스레드 생성 실패
        }
    }
    return POOL_SUCCESS; // 스레드풀 초기화 성공

}

/*
 * 스레드풀에서 실행시킬 함수와 인자의 주소를 넘겨주며 작업을 요청한다.
 * 스레드풀의 대기열이 꽉 찬 상황에서 flag이 POOL_NOWAIT이면 즉시 POOL_FULL을 리턴한다.
 * POOL_WAIT이면 대기열에 빈 자리가 나올 때까지 기다렸다가 넣고 나온다.
 * 작업 요청이 성공하면 POOL_SUCCESS를 그렇지 않으면 POOL_FAIL을 리턴한다.
 */
int pthread_pool_submit(pthread_pool_t *pool, void (*f)(void *p), void *p, int flag)
{
    if (pool->state != ON) {
        return POOL_FAIL; // 스레드풀이 실행 중이 아니면 실패
    }
    pthread_mutex_lock(&pool->mutex); // 대기열 접근을 위한 락 획득
    if (pool->q_len == pool->q_size) {
        if (flag != POOL_WAIT) {
            pthread_mutex_unlock(&pool->mutex);
            return POOL_FULL; // 대기열이 꽉 찼고, NOWAIT 플래그면 실패
        }
        while (pool->q_len == pool->q_size) {
            pthread_cond_wait(&pool->empty, &pool->mutex); // 빈 자리가 생길 때까지 대기
        }
        if(pool->state != ON) {
            pthread_mutex_unlock(&pool->mutex);
            return POOL_FAIL; // 대기중에 스레드풀이 종료되면 실패
        }
    }
    // 작업을 대기열에 추가
    int q_rear = (pool->q_front + pool->q_len) % pool->q_size;
    pool->q[q_rear].function = f;
    pool->q[q_rear].param = p;
    pool->q_len++; // 대기열 길이 증가
    pthread_cond_signal(&pool->full); // 대기열에 새 작업이 들어왔음을 알림
    pthread_mutex_unlock(&pool->mutex); // 락 해제
    return POOL_SUCCESS; // 작업 요청 성공  
}

/*
 * 스레드풀을 종료한다. 일꾼 스레드가 현재 작업 중이면 그 작업을 마치게 한다.
 * how의 값이 POOL_COMPLETE이면 대기열에 남아 있는 모든 작업을 마치고 종료한다.
 * POOL_DISCARD이면 대기열에 새 작업이 남아 있어도 더 이상 수행하지 않고 종료한다.
 * 메인(부모) 스레드는 종료된 일꾼 스레드와 조인한 후에 스레드풀에 할당된 자원을 반납한다.
 * 스레드를 종료시키기 위해 철회를 생각할 수 있으나 바람직하지 않다.
 * 락을 소유한 스레드를 중간에 철회하면 교착상태가 발생하기 쉽기 때문이다.
 * 종료가 완료되면 POOL_SUCCESS를 리턴한다.
 */
int pthread_pool_shutdown(pthread_pool_t *pool, int how)
{
    pthread_mutex_lock(&pool->mutex); // 대기열 접근을 위한 락 획득
    if (pool->state == OFF) {
        pthread_mutex_unlock(&pool->mutex);
        return POOL_FAIL; // 이미 종료된 스레드풀은 다시 종료할 수 없음
    }

    if (how == POOL_COMPLETE) {
        pool->state = STANDBY; // 스레드풀을 대기 상태로 변경
    }
    else if (how == POOL_DISCARD) {
        pool->q_len = 0; // 대기열의 모든 작업을 버림
        pool->q_front = 0; // 대기열 포인터 초기화
        pool->state = OFF; // 스레드풀 종료 상태로 변경
    }
    else {
        pthread_mutex_unlock(&pool->mutex);
        return POOL_FAIL; // 잘못된 종료 옵션
    }
    pthread_cond_broadcast(&pool->full); // 모든 일꾼 스레드에게 종료 알림
    pthread_cond_broadcast(&pool->empty); // 대기 중인 일꾼 스레드에게 알림
    pthread_mutex_unlock(&pool->mutex); // 락 해제
    for (int i = 0; i < pool->bee_size; i++) {
        pthread_join(pool->bee[i], NULL); // 모든 일꾼 스레드와 조인
    }
    pthread_mutex_lock(&pool->mutex); // 다시 락 획득
    pool->state = OFF; // 스레드풀 상태를 종료로 변경
    pthread_mutex_unlock(&pool->mutex); // 락 해제
    pthread_mutex_destroy(&pool->mutex); // 락 제거
    pthread_cond_destroy(&pool->full); // 조건 변수 제거
    pthread_cond_destroy(&pool->empty); // 조건 변수 제거
    free(pool->q); // 대기열 메모리 해제
    free(pool->bee); // 일꾼 스레드 메모리 해제
    pool->q = NULL; // 포인터 초기화
    pool->bee = NULL; // 포인터 초기화
    return POOL_SUCCESS; // 스레드풀 종료 성공
}
