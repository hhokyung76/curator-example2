package com.dmp.curatorexample;

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
import org.apache.zookeeper.CreateMode;

public class GettingStarted
{
	//    
	public CuratorFramework createClient(String connectionString)
	{
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
		return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
	}
	
	//    
	public CuratorFramework createClient(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs)
	{
		return CuratorFrameworkFactory.builder().connectString(connectionString).retryPolicy(retryPolicy).connectionTimeoutMs(connectionTimeoutMs).sessionTimeoutMs(sessionTimeoutMs).build();
	}
	
	//    
	public void create(CuratorFramework client, String path, String payload) throws Exception
	{
	    client.create().forPath(path, payload == null ? null : payload.getBytes());
	}
	
	//      
	public void createEphemeral(CuratorFramework client, String path, String payload) throws Exception
	{
		client.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload == null ? null : payload.getBytes());
	}
	
	//      
	public void setData(CuratorFramework client, String path, String payload) throws Exception
	{
		client.setData().forPath(path, payload == null ? null : payload.getBytes());
	}
	
	//        
	public void setDataAsync(CuratorFramework client, String path, String payload) throws Exception
	{
		CuratorListener listener = new CuratorListener()
		{
			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
			{
				System.out.println("receive");
			}
		};
		client.getCuratorListenable().addListener(listener);
		client.setData().inBackground().forPath(path, payload == null ? null : payload.getBytes());
	}
	
	//        £¬    
	public void setDataAsyncWithCallback(CuratorFramework client, BackgroundCallback callback, String path, String payload) throws Exception
	{
		client.setData().inBackground(callback).forPath(path, payload == null ? null : payload.getBytes());
	}
	
	//    
	public void delete(CuratorFramework client, String path) throws Exception
	{
		client.delete().forPath(path);
	}
	
	//    
	public void guaranteedDelete(CuratorFramework client, String path) throws Exception
	{
		client.delete().guaranteed().forPath(path);
	}

	//    
	public void watchData(CuratorFramework client, String path) throws Exception
	{
		NodeCache nc = new NodeCache(client, path);
		//nc.start(false);
		
		nc.getListenable().addListener(new NodeCacheListener()
		{
			public void nodeChanged() throws Exception
			{
				System.out.println(nc.getCurrentData().getPath() + ":" + new String(nc.getCurrentData().getData()));
			}
		});
		
		TimeUnit.SECONDS.sleep(20);
		nc.close();
	}
	
	public static void main(String[] args) {
		String zkConnString = "localhost:2181";
		CuratorFramework client = null;
		try {
			client = CuratorFrameworkFactory.newClient(zkConnString,
					new ExponentialBackoffRetry(1000, 3));
			client.getCuratorListenable().addListener(new CuratorListener() {
				@Override
				public void eventReceived(CuratorFramework client,
						CuratorEvent event) throws Exception {
					System.out.println("CuratorEvent: "
							+ event.getType().name());
				}
			});
		}catch (Exception e) {
			
		}
		GettingStarted star = new GettingStarted();
		try {
			star.watchChild(client, "/example/nodeCache");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//     
	public void watchChild(CuratorFramework client, String path) throws Exception
	{
		PathChildrenCache pcc = new PathChildrenCache(client, path, true);
		pcc.start();
		
		pcc.getListenable().addListener(new PathChildrenCacheListener()
		{
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
			{
				System.out.println(event.getData().getPath() + ":" + new String(event.getData().getData()));
			}
		});
		
		TimeUnit.SECONDS.sleep(20);
		pcc.close();
	}
	
	//   
	public void watchTree(CuratorFramework client, String path) throws Exception
	{
		TreeCache tc = TreeCache.newBuilder(client, path).build();
		tc.start();
		
		tc.getListenable().addListener(new TreeCacheListener() {
			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception
			{
				System.out.println(event.getData().getPath() + ":" + new String(event.getData().getData()));
			}
		});
		
		TimeUnit.SECONDS.sleep(20);
		tc.close();
	}
}
