package com.dmp.curatorexample;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class GettingStarted2 {
	//
	public CuratorFramework createClient(String connectionString) {
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
		return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
	}

	//
	public CuratorFramework createClient(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs,
			int sessionTimeoutMs) {
		return CuratorFrameworkFactory.builder().connectString(connectionString).retryPolicy(retryPolicy)
				.connectionTimeoutMs(connectionTimeoutMs).sessionTimeoutMs(sessionTimeoutMs).build();
	}

	//
	public void create(CuratorFramework client, String path, String payload) throws Exception {
		client.create().forPath(path, payload == null ? null : payload.getBytes());
	}

	//
	public void createEphemeral(CuratorFramework client, String path, String payload) throws Exception {
		client.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload == null ? null : payload.getBytes());
	}

	//
	public void setData(CuratorFramework client, String path, String payload) throws Exception {
		client.setData().forPath(path, payload == null ? null : payload.getBytes());
	}

	//
	public void setDataAsync(CuratorFramework client, String path, String payload) throws Exception {
		CuratorListener listener = new CuratorListener() {
			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
				System.out.println("receive");
			}
		};
		client.getCuratorListenable().addListener(listener);
		client.setData().inBackground().forPath(path, payload == null ? null : payload.getBytes());
	}

	// £¬
	public void setDataAsyncWithCallback(CuratorFramework client, BackgroundCallback callback, String path,
			String payload) throws Exception {
		client.setData().inBackground(callback).forPath(path, payload == null ? null : payload.getBytes());
	}

	//
	public void delete(CuratorFramework client, String path) throws Exception {
		client.delete().forPath(path);
	}

	//
	public void guaranteedDelete(CuratorFramework client, String path) throws Exception {
		client.delete().guaranteed().forPath(path);
	}

	//
	public void watchData(CuratorFramework client, String path) throws Exception {
		NodeCache nc = new NodeCache(client, path);
		// nc.start(false);

		nc.getListenable().addListener(new NodeCacheListener() {
			public void nodeChanged() throws Exception {
				System.out.println(nc.getCurrentData().getPath() + ":" + new String(nc.getCurrentData().getData()));
			}
		});

		TimeUnit.SECONDS.sleep(20);
		nc.close();
	}

	public static void main(String[] args) {
		String zkConnString = "localhost:2181";
		@SuppressWarnings("deprecation")
		NodeCache cache = null;
		CuratorFramework client = CuratorFrameworkFactory.newClient(zkConnString, new ExponentialBackoffRetry(1000, 3));
		client.getCuratorListenable().addListener(new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
				System.out.println("CuratorEvent Type: " + event.getType().name());
				System.out.println("CuratorEvent getPath: " + event.getPath());
				System.out.println("CuratorEvent getData: " + event.getData());
				
			}
		});
//		CuratorFramework client = newClient()
		client.start();
		AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
		String key = "/example/nodeCache";
		String expected = "my_value";

		async.create().forPath(key);

		List<String> changes = new ArrayList<>();

		async.watched().getData().forPath(key).event().thenAccept(watchedEvent -> {
			try {
				System.out.println("client.getData(): "+client.getData());
				System.out.println("client.getData().forPath(watchedEvent.getPath()): "+client.getData().forPath(watchedEvent.getPath()));
				changes.add(new String(client.getData().forPath(watchedEvent.getPath())));
			} catch (Exception e) {
				// fail ...
			}
		});
//
//		// Set data value for our key
//		async.setData().forPath(key, expected.getBytes());

//		await().until(() -> assertThat(changes.size()).isEqualTo(1));
	}

	//
	public void watchChild(CuratorFramework client, String path) throws Exception {
		PathChildrenCache pcc = new PathChildrenCache(client, path, true);
		pcc.start();

		pcc.getListenable().addListener(new PathChildrenCacheListener() {
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				System.out.println(event.getData().getPath() + ":" + new String(event.getData().getData()));
			}
		});

		TimeUnit.SECONDS.sleep(20);
		pcc.close();
	}

	//
	public void watchTree(CuratorFramework client, String path) throws Exception {
		TreeCache tc = TreeCache.newBuilder(client, path).build();
		tc.start();

		tc.getListenable().addListener(new TreeCacheListener() {
			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
				System.out.println(event.getData().getPath() + ":" + new String(event.getData().getData()));
			}
		});

		TimeUnit.SECONDS.sleep(20);
		tc.close();
	}
}
