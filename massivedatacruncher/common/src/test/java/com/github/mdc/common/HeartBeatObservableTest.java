package com.github.mdc.common;

import static org.junit.Assert.assertNotNull;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.github.mdc.common.ApplicationTask;
import com.github.mdc.common.HeartBeatObservable;
import com.github.mdc.common.JobStage;

public class HeartBeatObservableTest {

	@Test
	public void hboTestJobStage() throws Exception {
		HeartBeatObservable<JobStage> hobjs = new HeartBeatObservable<>();
		Queue<JobStage> queue = new LinkedBlockingQueue<>();
		hobjs.addPropertyChangeListener((event) ->queue.add((JobStage) event.getNewValue()));
		hobjs.start();
		hobjs.addToQueue(new JobStage());
		while(Objects.isNull(queue.peek())) {
			Thread.sleep(500);
		}
		JobStage jobstage = queue.poll();
		assertNotNull(jobstage);
	}
	
	@Test
	public void hboTestAppTask() throws Exception {
		HeartBeatObservable<ApplicationTask> hobat = new HeartBeatObservable<>();
		Queue<ApplicationTask> queue = new LinkedBlockingQueue<>();
		hobat.addPropertyChangeListener((event) ->queue.add((ApplicationTask) event.getNewValue()));
		hobat.start();
		hobat.addToQueue(new ApplicationTask());
		while(Objects.isNull(queue.peek())) {
			Thread.sleep(500);
		}
		ApplicationTask apptask = queue.poll();
		assertNotNull(apptask);
	}
	
	@Test
	public void hboTestJobStageAddToQueueBeforeStart() throws Exception {
		HeartBeatObservable<JobStage> hobjs = new HeartBeatObservable<>();
		Queue<JobStage> queue = new LinkedBlockingQueue<>();
		hobjs.addPropertyChangeListener((event) ->queue.add((JobStage) event.getNewValue()));
		hobjs.addToQueue(new JobStage());
		hobjs.start();
		
		while(Objects.isNull(queue.peek())) {
			Thread.sleep(500);
		}
		JobStage jobstage = queue.poll();
		assertNotNull(jobstage);
	}
	
	@Test
	public void hboTestAppTaskAddToQueueBeforeStart() throws Exception {
		HeartBeatObservable<ApplicationTask> hobat = new HeartBeatObservable<>();
		Queue<ApplicationTask> queue = new LinkedBlockingQueue<>();
		hobat.addPropertyChangeListener((event) ->queue.add((ApplicationTask) event.getNewValue()));
		hobat.addToQueue(new ApplicationTask());
		hobat.start();
		while(Objects.isNull(queue.peek())) {
			Thread.sleep(500);
		}
		ApplicationTask apptask = queue.poll();
		assertNotNull(apptask);
	}
	
	@Test
	public void hboTestJobStageMultipleObjects() throws Exception {
		HeartBeatObservable<JobStage> hobjs = new HeartBeatObservable<>();
		Queue<JobStage> queue = new LinkedBlockingQueue<>();
		hobjs.addPropertyChangeListener((event) ->queue.add((JobStage) event.getNewValue()));
		hobjs.start();
		int numobjects = 10;
		int currentobj = 0;
		while(currentobj<numobjects) {
			hobjs.addToQueue(new JobStage());
			currentobj++;
		}
		currentobj=0;
		while(!Objects.isNull(queue.peek())||currentobj<numobjects) {
			Thread.sleep(500);
			JobStage jobstage = queue.poll();
			if(!Objects.isNull(jobstage)) {
				assertNotNull(jobstage);
				currentobj++;
			}
		}
		
	}
	
	@Test
	public void hboTestAppTaskMultipleObjects() throws Exception {
		HeartBeatObservable<ApplicationTask> hobat = new HeartBeatObservable<>();
		Queue<ApplicationTask> queue = new LinkedBlockingQueue<>();
		hobat.addPropertyChangeListener((event) ->queue.add((ApplicationTask) event.getNewValue()));
		hobat.start();
		int numobjects = 10;
		int currentobj = 0;
		while(currentobj<numobjects) {
			hobat.addToQueue(new ApplicationTask());
			currentobj++;
		}
		currentobj=0;
		while(!Objects.isNull(queue.peek())||currentobj<numobjects) {
			Thread.sleep(500);
			ApplicationTask apptask = queue.poll();
			if(!Objects.isNull(apptask)) {
				assertNotNull(apptask);
				currentobj++;
			}
		}
	}
	
	@Test
	public void hboTestJobStageMultipleObjectsMultipleListeners() throws Exception {
		HeartBeatObservable<JobStage> hobjs = new HeartBeatObservable<>();
		Queue<JobStage> queue1 = new LinkedBlockingQueue<>();
		Queue<JobStage> queue2 = new LinkedBlockingQueue<>();
		hobjs.addPropertyChangeListener((event) ->queue1.add((JobStage) event.getNewValue()));
		hobjs.addPropertyChangeListener((event) ->queue2.add((JobStage) event.getNewValue()));
		hobjs.start();
		int numobjects = 10;
		int currentobj = 0;
		while(currentobj<numobjects) {
			hobjs.addToQueue(new JobStage());
			currentobj++;
		}
		currentobj=0;
		while(!Objects.isNull(queue1.peek())||currentobj<numobjects) {
			Thread.sleep(500);
			JobStage jobstage = queue1.poll();
			if(!Objects.isNull(jobstage)) {
				assertNotNull(jobstage);
				currentobj++;
			}
		}
		currentobj=0;
		while(!Objects.isNull(queue2.peek())||currentobj<numobjects) {
			Thread.sleep(500);
			JobStage jobstage = queue2.poll();
			if(!Objects.isNull(jobstage)) {
				assertNotNull(jobstage);
				currentobj++;
			}
		}
	}
	
	@Test
	public void hboTestAppTaskMultipleObjectsMultipleListeners() throws Exception {
		HeartBeatObservable<ApplicationTask> hobat = new HeartBeatObservable<>();
		Queue<ApplicationTask> queue1 = new LinkedBlockingQueue<>();
		Queue<ApplicationTask> queue2 = new LinkedBlockingQueue<>();
		hobat.addPropertyChangeListener((event) ->queue1.add((ApplicationTask) event.getNewValue()));
		hobat.addPropertyChangeListener((event) ->queue2.add((ApplicationTask) event.getNewValue()));
		hobat.start();
		int numobjects = 10;
		int currentobj = 0;
		while(currentobj<numobjects) {
			hobat.addToQueue(new ApplicationTask());
			currentobj++;
		}
		currentobj=0;
		while(!Objects.isNull(queue1.peek())||currentobj<numobjects) {
			Thread.sleep(500);
			ApplicationTask apptask = queue1.poll();
			if(!Objects.isNull(apptask)) {
				assertNotNull(apptask);
				currentobj++;
			}
		}
		currentobj=0;
		while(!Objects.isNull(queue2.peek())||currentobj<numobjects) {
			Thread.sleep(500);
			ApplicationTask apptask = queue2.poll();
			if(!Objects.isNull(apptask)) {
				assertNotNull(apptask);
				currentobj++;
			}
		}
	}
	
	
	@Test
	public void hboTestJobStageMultipleListeners() throws Exception {
		HeartBeatObservable<JobStage> hobjs = new HeartBeatObservable<>();
		Queue<JobStage> queue1 = new LinkedBlockingQueue<>();
		Queue<JobStage> queue2 = new LinkedBlockingQueue<>();
		hobjs.addPropertyChangeListener((event) ->queue1.add((JobStage) event.getNewValue()));
		hobjs.addPropertyChangeListener((event) ->queue2.add((JobStage) event.getNewValue()));
		hobjs.start();
		hobjs.addToQueue(new JobStage());
		while(Objects.isNull(queue1.peek())) {
			Thread.sleep(500);
		}
		JobStage jobstage = queue1.poll();
		assertNotNull(jobstage);
		while(Objects.isNull(queue2.peek())) {
			Thread.sleep(500);
		}
		jobstage = queue2.poll();
		assertNotNull(jobstage);
	}
	
	@Test
	public void hboTestAppTaskMultipleListeners() throws Exception {
		HeartBeatObservable<ApplicationTask> hobat = new HeartBeatObservable<>();
		Queue<ApplicationTask> queue1 = new LinkedBlockingQueue<>();
		Queue<ApplicationTask> queue2 = new LinkedBlockingQueue<>();
		hobat.addPropertyChangeListener((event) ->queue1.add((ApplicationTask) event.getNewValue()));
		hobat.addPropertyChangeListener((event) ->queue2.add((ApplicationTask) event.getNewValue()));
		hobat.start();
		hobat.addToQueue(new ApplicationTask());
		while(Objects.isNull(queue1.peek())) {
			Thread.sleep(500);
		}
		ApplicationTask apptask = queue1.poll();
		assertNotNull(apptask);
		while(Objects.isNull(queue2.peek())) {
			Thread.sleep(500);
		}
		apptask = queue2.poll();
		assertNotNull(apptask);
	}
}
